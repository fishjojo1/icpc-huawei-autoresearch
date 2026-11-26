// Local interactor + scorer for CF 2251A "Edge-Cloud Collaborative Scheduling".
//
// Reimplements the judge's discrete-event simulation exactly as specified in
// docs/statement.txt, spawns a solution process, speaks the interactive
// protocol with it, validates every assignment, and reports the score.
//
//   ./interactor <test_file> [options] -- <solution cmd> [args...]
//
// Options:
//   --transcript <file>   dump the full interaction (both directions)
//   --read-timeout <ms>   per-read timeout waiting on the solution (default 300000).
//                         This is a hang detector, not the time limit: TLE is
//                         judged on the solution's CPU time via --tl. Keep it
//                         generous, because a wall-clock timeout fires
//                         spuriously when many benchmarks run in parallel and
//                         a spurious VIOLATION silently zeroes a test.
//   --tl <ms>             solution CPU-time limit, mirrors the judge (default 15000)
//   --quiet               only emit the result JSON
//
// Result is a single JSON line on stdout.
//
// Test file format (see tools/gen.py):
//   K S latency_in_ms bandwidth_gbps bytes_per_token num_layers
//   SLO1 SLO2 tp_UB tp_base dist_base w_tp w_c
//   N
//   N rows: batch_size prefill_pre prefill_proc prefill_post decode_pre decode_proc decode_post
//   R
//   R rows: arrival_time L_in L_out
//
// The first N+3 lines are forwarded verbatim to the solution; the workload is hidden.

#include <algorithm>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <queue>
#include <string>
#include <vector>

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

// ---------------------------------------------------------------- utilities

// Must not truncate: an echoed D PRE spec can list up to R=2000 request ids,
// and a silently clipped frame desynchronises the solution instead of failing
// it honestly.
static std::string fmt(const char *f, ...) {
    va_list ap;
    va_start(ap, f);
    va_list ap2;
    va_copy(ap2, ap);
    int n = vsnprintf(nullptr, 0, f, ap);
    va_end(ap);
    std::string s;
    if (n > 0) {
        s.resize((size_t)n);
        vsnprintf(&s[0], (size_t)n + 1, f, ap2);
    }
    va_end(ap2);
    return s;
}

static double nowMs() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return tv.tv_sec * 1000.0 + tv.tv_usec / 1000.0;
}

// Piecewise-linear lookup over (size, value) pairs sorted ascending by size.
// Below the smallest size use the first value, above the largest use the last.
static double lookupCol(const std::vector<std::pair<double, double>> &v, double x) {
    if (v.empty()) return 1.0;  // guaranteed not to happen by the statement
    if (x <= v.front().first) return v.front().second;
    if (x >= v.back().first) return v.back().second;
    size_t lo = 0, hi = v.size() - 1;
    while (hi - lo > 1) {
        size_t mid = (lo + hi) / 2;
        if (v[mid].first <= x) lo = mid; else hi = mid;
    }
    if (v[lo].first == x) return v[lo].second;
    double t = (x - v[lo].first) / (v[hi].first - v[lo].first);
    return v[lo].second + t * (v[hi].second - v[lo].second);
}

// ------------------------------------------------------------------- model

struct Params {
    int K = 1;
    double S = 1, lat = 1, bw = 1;
    double bpt = 1;
    int num_layers = 1;
    double SLO1 = 1, SLO2 = 1, tp_UB = 1, tp_base = 0, dist_base = 0, w_tp = 0.5, w_c = 0.5;
};

enum Col { PRE_PRE = 0, PRE_PROC, PRE_POST, DEC_PRE, DEC_PROC, DEC_POST };

// Prefill progress of one request.
enum PState {
    P_UNARRIVED = 0,
    P_NEED_PRE,     // ARR delivered, P PRE assignable
    P_PRE_RUN,
    P_WAIT_UP,      // prefill UP transfer in flight
    P_NEED_PROC,    // next piece assignable
    P_PROC_RUN,
    P_WAIT_DOWN,
    P_NEED_POST,
    P_POST_RUN,
    P_DONE
};

// Decode progress within the current output iteration.
enum DState {
    D_NONE = 0,
    D_NEED_PRE,
    D_PRE_RUN,
    D_WAIT_UP,
    D_NEED_PROC,
    D_PROC_RUN,
    D_WAIT_DOWN,
    D_NEED_POST,
    D_POST_RUN,
    D_FINISHED
};

struct Req {
    double arrival = 0;
    int Lin = 1, Lout = 1;
    int remote = -1;
    int next_layer = 0;
    int tokens = 0;
    PState p = P_UNARRIVED;
    DState d = D_NONE;
    double tdr = 0;
    double last_token = 0;
};

enum TaskType { T_PPRE = 0, T_PPROC, T_PPOST, T_DPRE, T_DPROC, T_DPOST };

struct Task {
    TaskType type;
    int remote = -1;   // owning remote for PROC tasks / assigned remote field
    int ls = 0, le = 0;
    std::vector<int> rids;
    std::string spec;  // canonical echo, e.g. "P PROC 0 4 0 0"
    double dur = 0;
    int server = -1;   // -1 = local (E), else remote index
};

enum EvKind { EV_ARR = 0, EV_TDN, EV_XDN };

struct Ev {
    double t;
    long long seq;
    EvKind kind;
    // ARR
    int rid = -1;
    // TDN
    Task task;
    // XDN
    int dir = 0;  // 0 = UP, 1 = DOWN
    int xremote = 0;
    double xsize = 0;
    bool xprefill = false;
    std::vector<int> xrids;
};

struct EvCmp {
    bool operator()(const Ev &a, const Ev &b) const {
        if (a.t != b.t) return a.t > b.t;
        return a.seq > b.seq;
    }
};

// ------------------------------------------------------- child process I/O

struct Child {
    pid_t pid = -1;
    int wfd = -1, rfd = -1;
    FILE *out = nullptr;
    std::string buf;
    size_t pos = 0;
    bool eof = false;
    int timeout_ms = 30000;

    bool fill() {
        if (eof) return false;
        struct pollfd p{rfd, POLLIN, 0};
        int r = poll(&p, 1, timeout_ms);
        if (r <= 0) { eof = true; return false; }
        char tmp[65536];
        ssize_t n = read(rfd, tmp, sizeof(tmp));
        if (n <= 0) { eof = true; return false; }
        buf.assign(tmp, tmp + n);
        pos = 0;
        return true;
    }
    int getch() {
        while (pos >= buf.size()) {
            if (!fill()) return -1;
        }
        return (unsigned char)buf[pos++];
    }
    bool token(std::string &out_) {
        int c;
        do {
            c = getch();
            if (c < 0) return false;
        } while (c == ' ' || c == '\n' || c == '\r' || c == '\t');
        out_.clear();
        while (c > 0 && c != ' ' && c != '\n' && c != '\r' && c != '\t') {
            out_.push_back((char)c);
            c = getch();
        }
        return true;
    }
};

// ------------------------------------------------------------------- main

static FILE *g_trans = nullptr;
static bool g_quiet = false;

struct Interactor {
    Params P;
    std::vector<std::pair<double, double>> col[6];
    std::vector<Req> req;
    int R = 0;

    Child ch;
    std::priority_queue<Ev, std::vector<Ev>, EvCmp> pq;
    long long seq = 0;

    bool local_busy = false;
    std::vector<char> rem_busy;
    double link_free[2] = {0, 0};

    // metrics
    double tdr_sum = 0;
    double gap_sum = 0;
    long long gap_cnt = 0;
    double last_token_time = 0;
    double first_arrival = 0;
    long long sum_lout = 0;
    long long finished = 0;
    long long frames = 0, events_out = 0;

    std::string verdict = "OK";
    std::string err;

    // ---- table lookups
    double tPrePre(int Lin) { return lookupCol(col[PRE_PRE], Lin); }
    double tPreProc(int Lin) { return lookupCol(col[PRE_PROC], Lin); }
    double tPrePost(int Lin) { return lookupCol(col[PRE_POST], Lin); }
    double tDecPre(int m) { return lookupCol(col[DEC_PRE], m); }
    double tDecProc(int m) { return lookupCol(col[DEC_PROC], m); }
    double tDecPost(int m) { return lookupCol(col[DEC_POST], m); }

    double transferDur(double len) { return P.lat + 8.0 * len * P.bpt / (P.bw * 1e6); }

    void pushEv(Ev e) { e.seq = seq++; pq.push(e); }

    void queueTransfer(int dir, int remote, double len, bool prefill,
                       const std::vector<int> &rids, double now) {
        double dur = transferDur(len);
        double start = std::max(link_free[dir], now);
        double fin = start + dur;
        link_free[dir] = fin;
        Ev e;
        e.t = fin;
        e.kind = EV_XDN;
        e.dir = dir;
        e.xremote = remote;
        e.xsize = len * P.bpt;
        e.xprefill = prefill;
        e.xrids = rids;
        pushEv(e);
    }

    bool fail(const std::string &m) {
        verdict = "VIOLATION";
        err = m;
        return false;
    }

    // ------------------------------------------------------------ loading
    bool load(const char *path) {
        FILE *f = fopen(path, "r");
        if (!f) { verdict = "IO"; err = "cannot open test file"; return false; }
        if (fscanf(f, "%d %lf %lf %lf %lf %d", &P.K, &P.S, &P.lat, &P.bw, &P.bpt, &P.num_layers) != 6)
            { verdict = "IO"; err = "bad param line 1"; fclose(f); return false; }
        if (fscanf(f, "%lf %lf %lf %lf %lf %lf %lf", &P.SLO1, &P.SLO2, &P.tp_UB, &P.tp_base,
                   &P.dist_base, &P.w_tp, &P.w_c) != 7)
            { verdict = "IO"; err = "bad param line 2"; fclose(f); return false; }
        int N;
        if (fscanf(f, "%d", &N) != 1) { verdict = "IO"; err = "bad N"; fclose(f); return false; }
        table_rows.resize(N);
        for (int i = 0; i < N; i++) {
            double bs, v[6];
            if (fscanf(f, "%lf %lf %lf %lf %lf %lf %lf", &bs, &v[0], &v[1], &v[2], &v[3], &v[4], &v[5]) != 7)
                { verdict = "IO"; err = "bad table row"; fclose(f); return false; }
            table_rows[i].first = bs;
            for (int c = 0; c < 6; c++) {
                table_rows[i].second[c] = v[c];
                if (v[c] >= 0) col[c].push_back({bs, v[c]});
            }
        }
        for (int c = 0; c < 6; c++) std::sort(col[c].begin(), col[c].end());
        if (fscanf(f, "%d", &R) != 1) { verdict = "IO"; err = "bad R"; fclose(f); return false; }
        req.resize(R);
        for (int i = 0; i < R; i++) {
            if (fscanf(f, "%lf %d %d", &req[i].arrival, &req[i].Lin, &req[i].Lout) != 3)
                { verdict = "IO"; err = "bad request row"; fclose(f); return false; }
            sum_lout += req[i].Lout;
        }
        fclose(f);
        return true;
    }
    std::vector<std::pair<double, std::vector<double>>> table_rows_storage;
    struct Row { double first; double second[6]; };
    std::vector<Row> table_rows;

    // ------------------------------------------------------------ spawning
    bool spawn(char **argv) {
        int toChild[2], fromChild[2];
        if (pipe(toChild) || pipe(fromChild)) { verdict = "IO"; err = "pipe failed"; return false; }
        pid_t pid = fork();
        if (pid < 0) { verdict = "IO"; err = "fork failed"; return false; }
        if (pid == 0) {
            dup2(toChild[0], 0);
            dup2(fromChild[1], 1);
            close(toChild[0]); close(toChild[1]);
            close(fromChild[0]); close(fromChild[1]);
            execvp(argv[0], argv);
            _exit(127);
        }
        close(toChild[0]);
        close(fromChild[1]);
        ch.pid = pid;
        ch.wfd = toChild[1];
        ch.rfd = fromChild[0];
        ch.out = fdopen(ch.wfd, "w");
        return true;
    }

    void send(const std::string &s) {
        if (g_trans) fprintf(g_trans, "%s", s.c_str());
        if (ch.out) fputs(s.c_str(), ch.out);
    }
    void flushSend() { if (ch.out) fflush(ch.out); }

    // --------------------------------------------------------- header send
    void sendHeader() {
        send(fmt("%d %.9f %.9f %.9f %.0f %d\n", P.K, P.S, P.lat, P.bw, P.bpt, P.num_layers));
        send(fmt("%.9f %.9f %.9f %.9f %.9f %.9f %.9f\n", P.SLO1, P.SLO2, P.tp_UB, P.tp_base,
                 P.dist_base, P.w_tp, P.w_c));
        send(fmt("%d\n", (int)table_rows.size()));
        for (auto &r : table_rows) {
            std::string line = fmt("%.0f", r.first);
            for (int c = 0; c < 6; c++) {
                if (r.second[c] < 0) line += " -1";
                else line += fmt(" %.9f", r.second[c]);
            }
            send(line + "\n");
        }
        flushSend();
    }

    // ------------------------------------------------------------- effects
    // Apply a completed task's effects. Returns FIN rids produced.
    void applyTDN(const Task &t, double now, std::vector<int> &fins) {
        if (t.server < 0) local_busy = false; else rem_busy[t.server] = 0;
        switch (t.type) {
            case T_PPRE: {
                int r = t.rids[0];
                req[r].remote = t.remote;
                req[r].p = P_WAIT_UP;
                queueTransfer(0, t.remote, req[r].Lin, true, {r}, now);
                break;
            }
            case T_PPROC: {
                int r = t.rids[0];
                req[r].next_layer = t.le;
                if (t.le == P.num_layers) {
                    req[r].p = P_WAIT_DOWN;
                    queueTransfer(1, req[r].remote, req[r].Lin, true, {r}, now);
                } else {
                    req[r].p = P_NEED_PROC;
                }
                break;
            }
            case T_PPOST: {
                int r = t.rids[0];
                req[r].p = P_DONE;
                req[r].d = D_NEED_PRE;
                req[r].tdr = now - req[r].arrival;
                tdr_sum += req[r].tdr;
                break;
            }
            case T_DPRE: {
                for (int k = 0; k < P.K; k++) {
                    std::vector<int> mem;
                    for (int r : t.rids) if (req[r].remote == k) mem.push_back(r);
                    if (mem.empty()) continue;
                    queueTransfer(0, k, (double)mem.size(), false, mem, now);
                }
                for (int r : t.rids) req[r].d = D_WAIT_UP;
                break;
            }
            case T_DPROC: {
                queueTransfer(1, t.remote, (double)t.rids.size(), false, t.rids, now);
                for (int r : t.rids) req[r].d = D_WAIT_DOWN;
                break;
            }
            case T_DPOST: {
                for (int r : t.rids) {
                    if (req[r].tokens > 0) {
                        gap_sum += now - req[r].last_token;
                        gap_cnt++;
                    }
                    req[r].tokens++;
                    req[r].last_token = now;
                    last_token_time = std::max(last_token_time, now);
                    if (req[r].tokens >= req[r].Lout) {
                        req[r].d = D_FINISHED;
                        finished++;
                        fins.push_back(r);
                    } else {
                        req[r].d = D_NEED_PRE;
                    }
                }
                break;
            }
        }
    }

    void applyXDN(const Ev &e) {
        if (e.xprefill) {
            int r = e.xrids[0];
            if (e.dir == 0) req[r].p = P_NEED_PROC;
            else req[r].p = P_NEED_POST;
        } else {
            for (int r : e.xrids) req[r].d = (e.dir == 0) ? D_NEED_PROC : D_NEED_POST;
        }
    }

    // ------------------------------------------------------ response parse
    bool readResponse(double t) {
        std::string tok;
        if (!ch.token(tok)) return fail("solution closed its output / timed out");
        long long n = 0;
        try { n = std::stoll(tok); } catch (...) { return fail("unparsable assignment count '" + tok + "'"); }
        if (n < 0 || n > P.K + 1) return fail(fmt("assignment count %lld out of [0,%d]", n, P.K + 1));
        if (g_trans) fprintf(g_trans, "< %lld\n", n);

        bool used_local = false;
        std::vector<char> used_rem(P.K, 0);

        for (long long i = 0; i < n; i++) {
            std::string sv;
            if (!ch.token(sv)) return fail("truncated response");
            int server;
            if (sv == "E") server = -1;
            else if (sv.size() >= 2 && sv[0] == 'C') {
                try { server = std::stoi(sv.substr(1)); } catch (...) { return fail("bad server '" + sv + "'"); }
                if (server < 0 || server >= P.K) return fail("server index out of range: " + sv);
            } else return fail("bad server token '" + sv + "'");

            std::string a, b;
            if (!ch.token(a) || !ch.token(b)) return fail("truncated task spec");
            if (a != "P" && a != "D") return fail("bad step letter '" + a + "'");

            Task task;
            task.server = server;

            auto rdInt = [&](long long &v) -> bool {
                std::string s;
                if (!ch.token(s)) return false;
                try { v = std::stoll(s); } catch (...) { return false; }
                return true;
            };

            if (a == "P" && b == "PRE") {
                long long rm, rid;
                if (!rdInt(rm) || !rdInt(rid)) return fail("truncated P PRE");
                if (rm < 0 || rm >= P.K) return fail("P PRE remote out of [0,K)");
                if (rid < 0 || rid >= R) return fail("P PRE unknown rid");
                if (req[rid].p != P_NEED_PRE) return fail(fmt("P PRE on rid %lld not in NEED_PRE state", rid));
                if (server != -1) return fail("P PRE must run on E");
                task.type = T_PPRE; task.remote = (int)rm; task.rids = {(int)rid};
                task.spec = fmt("P PRE %lld %lld", rm, rid);
                task.dur = tPrePre(req[rid].Lin);
            } else if (a == "P" && b == "PROC") {
                long long ls, le, rm, rid;
                if (!rdInt(ls) || !rdInt(le) || !rdInt(rm) || !rdInt(rid)) return fail("truncated P PROC");
                if (rid < 0 || rid >= R) return fail("P PROC unknown rid");
                if (req[rid].p != P_NEED_PROC) return fail(fmt("P PROC on rid %lld not ready", rid));
                if (rm != req[rid].remote) return fail("P PROC wrong remote");
                if (server != (int)rm) return fail("P PROC must run on its assigned remote");
                if (ls != req[rid].next_layer) return fail("P PROC piece not gap-free/ascending");
                if (le <= ls || le > P.num_layers) return fail("P PROC illegal piece range");
                task.type = T_PPROC; task.remote = (int)rm; task.ls = (int)ls; task.le = (int)le;
                task.rids = {(int)rid};
                task.spec = fmt("P PROC %lld %lld %lld %lld", ls, le, rm, rid);
                task.dur = (double)(le - ls) / P.num_layers * tPreProc(req[rid].Lin);
            } else if (a == "P" && b == "POST") {
                long long rm, rid;
                if (!rdInt(rm) || !rdInt(rid)) return fail("truncated P POST");
                if (rid < 0 || rid >= R) return fail("P POST unknown rid");
                if (req[rid].p != P_NEED_POST) return fail(fmt("P POST on rid %lld not ready", rid));
                if (rm != req[rid].remote) return fail("P POST wrong remote field");
                if (server != -1) return fail("P POST must run on E");
                task.type = T_PPOST; task.remote = (int)rm; task.rids = {(int)rid};
                task.spec = fmt("P POST %lld %lld", rm, rid);
                task.dur = tPrePost(req[rid].Lin);
            } else if (a == "D") {
                long long rmField, m;
                if (!rdInt(rmField) || !rdInt(m)) return fail("truncated D task header");
                if (m < 1) return fail("group size < 1");
                std::vector<int> ids((size_t)m);
                for (long long j = 0; j < m; j++) {
                    long long v;
                    if (!rdInt(v)) return fail("truncated group member list");
                    if (v < 0 || v >= R) return fail("group member unknown rid");
                    ids[j] = (int)v;
                }
                std::vector<int> sorted = ids;
                std::sort(sorted.begin(), sorted.end());
                if (std::adjacent_find(sorted.begin(), sorted.end()) != sorted.end())
                    return fail("duplicate request ids in group");

                DState need;
                if (b == "PRE") need = D_NEED_PRE;
                else if (b == "PROC") need = D_NEED_PROC;
                else if (b == "POST") need = D_NEED_POST;
                else return fail("bad D step '" + b + "'");

                for (int r : ids) {
                    if (req[r].p != P_DONE) return fail(fmt("rid %d has not finished its input stage", r));
                    if (req[r].d == D_FINISHED) return fail(fmt("rid %d already finished (FIN)", r));
                    if (req[r].d != need) return fail(fmt("rid %d not at step D %s", r, b.c_str()));
                }
                if (b == "PROC") {
                    if (rmField < 0 || rmField >= P.K) return fail("D PROC remote out of range");
                    if (server != (int)rmField) return fail("D PROC must run on <remote>");
                    for (int r : ids)
                        if (req[r].remote != (int)rmField) return fail("D PROC member on another remote");
                } else {
                    if (rmField != -1) return fail(fmt("D %s must use -1 as its remote field", b.c_str()));
                    if (server != -1) return fail(fmt("D %s must run on E", b.c_str()));
                }

                std::string ids_s;
                for (int r : ids) ids_s += fmt(" %d", r);
                task.rids = ids;
                task.remote = (int)rmField;
                task.spec = fmt("D %s %lld %lld", b.c_str(), rmField, m) + ids_s;
                if (b == "PRE") { task.type = T_DPRE; task.dur = tDecPre((int)m); }
                else if (b == "PROC") { task.type = T_DPROC; task.dur = tDecProc((int)m); }
                else { task.type = T_DPOST; task.dur = tDecPost((int)m); }
            } else return fail("bad task spec");

            // resource availability
            if (server == -1) {
                if (local_busy) return fail("local computer E is busy");
                if (used_local) return fail("two tasks assigned to E in one response");
                used_local = true;
            } else {
                if (rem_busy[server]) return fail(fmt("remote C%d is busy", server));
                if (used_rem[server]) return fail(fmt("two tasks assigned to C%d in one response", server));
                used_rem[server] = 1;
            }

            // commit: mark busy + in-flight and schedule the TDN
            if (server == -1) local_busy = true; else rem_busy[server] = 1;
            switch (task.type) {
                case T_PPRE: req[task.rids[0]].p = P_PRE_RUN; break;
                case T_PPROC: req[task.rids[0]].p = P_PROC_RUN; break;
                case T_PPOST: req[task.rids[0]].p = P_POST_RUN; break;
                case T_DPRE: for (int r : task.rids) req[r].d = D_PRE_RUN; break;
                case T_DPROC: for (int r : task.rids) req[r].d = D_PROC_RUN; break;
                case T_DPOST: for (int r : task.rids) req[r].d = D_POST_RUN; break;
            }
            Ev e;
            e.t = t + P.S + task.dur;
            e.kind = EV_TDN;
            e.task = task;
            pushEv(e);
            if (g_trans) fprintf(g_trans, "< %s %s\n", sv.c_str(), task.spec.c_str());
        }
        return true;
    }

    // ---------------------------------------------------------------- run
    bool run() {
        rem_busy.assign(P.K, 0);
        first_arrival = R ? req[0].arrival : 0;
        for (int i = 0; i < R; i++) {
            Ev e;
            e.t = req[i].arrival;
            e.kind = EV_ARR;
            e.rid = i;
            pushEv(e);
            first_arrival = std::min(first_arrival, req[i].arrival);
        }
        sendHeader();

        while (finished < R) {
            if (pq.empty()) { verdict = "STUCK"; err = "no future event with unfinished requests"; return false; }
            double t = pq.top().t;
            std::vector<Ev> batch;
            while (!pq.empty() && pq.top().t == t) { batch.push_back(pq.top()); pq.pop(); }

            std::vector<std::string> lines;
            for (auto &e : batch) {
                if (e.kind == EV_ARR) {
                    req[e.rid].p = P_NEED_PRE;
                    lines.push_back(fmt("ARR %d %d", e.rid, req[e.rid].Lin));
                } else if (e.kind == EV_TDN) {
                    std::vector<int> fins;
                    applyTDN(e.task, t, fins);
                    std::string sv = e.task.server < 0 ? std::string("E") : fmt("C%d", e.task.server);
                    lines.push_back(fmt("TDN %s %s %.9f", sv.c_str(), e.task.spec.c_str(), e.task.dur));
                    for (int r : fins) lines.push_back(fmt("FIN %d", r));
                } else {
                    applyXDN(e);
                    std::string ids_s;
                    for (int r : e.xrids) ids_s += fmt(" %d", r);
                    lines.push_back(fmt("XDN %s %d %.0f %s %d%s", e.dir == 0 ? "UP" : "DOWN",
                                        e.xremote, e.xsize, e.xprefill ? "PRE" : "DEC",
                                        (int)e.xrids.size(), ids_s.c_str()));
                }
            }

            std::string frame = fmt("%.9f\n%d\n", t, (int)lines.size());
            for (auto &l : lines) frame += l + "\n";
            send(frame);
            flushSend();
            frames++;
            events_out += (long long)lines.size();

            if (finished >= R) {
                // Judge still reads one final response before sending END.
                if (!readResponse(t)) return false;
                break;
            }
            if (!readResponse(t)) return false;
        }
        send("END\n");
        flushSend();
        return true;
    }

    // ------------------------------------------------------------- scoring
    struct Result {
        double tp = 0, tdr = 0, tpot = 0, dist = 0;
        double tp_comp = 0, dist_comp = 0, norm = 0, score = 0;
        // Same formula without the [0,1] clamps. The official score saturates
        // at 0 on hard tests, which hides real progress from a search loop;
        // the unclamped value keeps a gradient there. Never report it as the
        // contest score.
        double score_unclamped = 0;
        double makespan = 0, tp_ub_est = 0;
    } res;

    // Analytic ceiling on sustainable token rate: the tightest of the local
    // computer, the remote pool, and each link direction, maximised over batch
    // size. Used only to fill tp_UB when generating tests locally.
    double estimateTpUB() {
        std::vector<double> cands;
        for (auto &pr : col[DEC_PRE]) cands.push_back(pr.first);
        for (auto &pr : col[DEC_PROC]) cands.push_back(pr.first);
        for (auto &pr : col[DEC_POST]) cands.push_back(pr.first);
        cands.push_back(1);
        cands.push_back(R);
        std::sort(cands.begin(), cands.end());
        double best = 0;
        for (double B : cands) {
            if (B < 1) continue;
            if (B > R) B = R;
            double local = B / (2 * P.S + tDecPre(B) + tDecPost(B));
            double remote = P.K * B / (P.S + tDecProc(B));
            double up = B / transferDur(B);
            double down = B / transferDur(B);
            double lim = std::min(std::min(local, remote), std::min(up, down));
            best = std::max(best, lim);
        }
        return best;
    }

    void score() {
        res.makespan = last_token_time - first_arrival;
        res.tp = res.makespan > 0 ? (double)sum_lout / res.makespan : 0;
        res.tdr = R ? tdr_sum / R : 0;
        res.tpot = gap_cnt ? gap_sum / gap_cnt : 0;
        double ex1 = std::max(0.0, (res.tdr - P.SLO1) / P.SLO1);
        double ex2 = std::max(0.0, (res.tpot - P.SLO2) / P.SLO2);
        res.dist = std::sqrt(ex1 * ex1 + ex2 * ex2);
        res.tp_ub_est = estimateTpUB();

        if (verdict != "OK") { res.score = 0; res.score_unclamped = 0; return; }
        double den = P.tp_UB - P.tp_base;
        double tp_raw = den > 0 ? (res.tp - P.tp_base) / den : 0.0;
        res.tp_comp = std::max(0.0, std::min(1.0, tp_raw));
        double dist_raw;
        if (P.dist_base > 0) {
            dist_raw = 1.0 - res.dist / P.dist_base;
            res.dist_comp = std::max(0.0, dist_raw);
        } else {
            dist_raw = (res.dist == 0) ? 1.0 : -res.dist;
            res.dist_comp = (res.dist == 0) ? 1.0 : 0.0;
        }
        res.norm = P.w_tp * res.tp_comp + P.w_c * res.dist_comp;
        res.score = 1000.0 * res.norm;
        res.score_unclamped = 1000.0 * (P.w_tp * tp_raw + P.w_c * dist_raw);
    }
};

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    if (argc < 2) {
        fprintf(stderr, "usage: interactor <test_file> [--transcript f] [--read-timeout ms] [--tl ms] [--quiet] -- <solution cmd...>\n");
        return 2;
    }
    const char *testfile = argv[1];
    int read_timeout = 300000, tl = 15000;
    int solStart = -1;
    for (int i = 2; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--") { solStart = i + 1; break; }
        else if (a == "--transcript" && i + 1 < argc) g_trans = fopen(argv[++i], "w");
        else if (a == "--read-timeout" && i + 1 < argc) read_timeout = atoi(argv[++i]);
        else if (a == "--tl" && i + 1 < argc) tl = atoi(argv[++i]);
        else if (a == "--quiet") g_quiet = true;
        else { fprintf(stderr, "unknown option %s\n", a.c_str()); return 2; }
    }
    if (solStart < 0 || solStart >= argc) { fprintf(stderr, "no solution command given (use -- <cmd>)\n"); return 2; }

    Interactor I;
    I.ch.timeout_ms = read_timeout;
    if (!I.load(testfile)) {
        printf("{\"verdict\":\"%s\",\"error\":\"%s\",\"score\":0}\n", I.verdict.c_str(), I.err.c_str());
        return 1;
    }
    double wall0 = nowMs();
    if (!I.spawn(argv + solStart)) {
        printf("{\"verdict\":\"%s\",\"error\":\"%s\",\"score\":0}\n", I.verdict.c_str(), I.err.c_str());
        return 1;
    }
    bool ok = I.run();
    (void)ok;

    if (I.ch.out) fclose(I.ch.out);
    close(I.ch.rfd);
    int status = 0;
    struct rusage ru;
    memset(&ru, 0, sizeof(ru));
    // Give the solution a moment to exit on its own, then reap it.
    for (int i = 0; i < 200; i++) {
        pid_t w = wait4(I.ch.pid, &status, WNOHANG, &ru);
        if (w == I.ch.pid) { I.ch.pid = -1; break; }
        usleep(5000);
    }
    if (I.ch.pid != -1) {
        kill(I.ch.pid, SIGKILL);
        wait4(I.ch.pid, &status, 0, &ru);
    }
    double wall = nowMs() - wall0;
    double sol_user = ru.ru_utime.tv_sec * 1000.0 + ru.ru_utime.tv_usec / 1000.0;
    double sol_sys = ru.ru_stime.tv_sec * 1000.0 + ru.ru_stime.tv_usec / 1000.0;
    // System time balloons when many benchmarks run in parallel (pipe churn and
    // context switches on a contended box), so a parallel run can report several
    // times the CPU it uses serially. Callers must confirm any TLE with a serial
    // re-run before believing it.
    double sol_cpu = sol_user + sol_sys;
#if defined(__APPLE__)
    double sol_mem_mb = ru.ru_maxrss / (1024.0 * 1024.0);
#else
    double sol_mem_mb = ru.ru_maxrss / 1024.0;
#endif
    if (I.verdict == "OK" && sol_mem_mb > 256.0) {
        I.verdict = "MLE";
        I.err = fmt("solution used %.1f MB memory", sol_mem_mb);
    }
    if (I.verdict == "OK" && sol_cpu > tl) { I.verdict = "TLE"; I.err = fmt("solution used %.0f ms CPU", sol_cpu); }
    if (I.verdict == "OK" && WIFSIGNALED(status)) { I.verdict = "CRASH"; I.err = fmt("signal %d", WTERMSIG(status)); }
    I.score();
    if (g_trans) fclose(g_trans);

    printf("{\"verdict\":\"%s\",\"error\":\"%s\",\"score\":%.6f,\"score_unclamped\":%.6f,"
           "\"norm\":%.9f,\"tp\":%.9f,\"tp_comp\":%.9f,\"tdr\":%.9f,\"tpot\":%.9f,\"dist\":%.9f,"
           "\"dist_comp\":%.9f,\"makespan\":%.9f,\"tp_ub_est\":%.9f,"
           "\"frames\":%lld,\"events\":%lld,\"sol_cpu_ms\":%.1f,\"wall_ms\":%.1f,"
           "\"sol_user_ms\":%.1f,\"sol_sys_ms\":%.1f,"
           "\"sol_mem_mb\":%.1f,\"R\":%d,\"sum_lout\":%lld}\n",
           I.verdict.c_str(), I.err.c_str(), I.res.score, I.res.score_unclamped,
           I.res.norm, I.res.tp, I.res.tp_comp,
           I.res.tdr, I.res.tpot, I.res.dist, I.res.dist_comp, I.res.makespan, I.res.tp_ub_est,
           I.frames, I.events_out, sol_cpu, wall, sol_user, sol_sys,
           sol_mem_mb, I.R, I.sum_lout);
    return I.verdict == "OK" ? 0 : 1;
}
