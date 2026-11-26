// Reference scheduler for CF 2251A (defines tp_base / dist_base locally).
//
// THE FULLY SERIALIZED REFERENCE: one request at a time, start to finish.
// A request's whole prefill chain runs, then its L_out decode rounds at group
// size 1, and only when it has finished does the next request begin. Nothing
// ever overlaps: at most one task or one transfer is in flight in the whole
// system. A request never starts before its own arrival, so an idle machine
// simply waits for the next ARR.
// Once a suite is calibrated this file must not change.
//
// The schedule is trivial, so no ready-buckets are needed: exactly one request
// is live and exactly one of its steps is outstanding, and every frame either
// advances that step or is an arrival we queue.

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <string>
#include <vector>

#include <unistd.h>

// ------------------------------------------------------------ fast input
//
// NOTE: this is an interactive problem, so input must be consumed with read(),
// not fread(). fread() blocks until its whole buffer is filled, which
// deadlocks against an interactor that is waiting for our response.

namespace io {
static char buf[1 << 16];
static size_t len = 0, pos = 0;

static inline int gc() {
    if (pos == len) {
        ssize_t got = ::read(0, buf, sizeof(buf));
        if (got <= 0) return -1;
        len = (size_t)got;
        pos = 0;
    }
    return (unsigned char)buf[pos++];
}
static bool token(std::string &out) {
    int c;
    do {
        c = gc();
        if (c < 0) return false;
    } while (c == ' ' || c == '\n' || c == '\r' || c == '\t');
    out.clear();
    while (c > 0 && c != ' ' && c != '\n' && c != '\r' && c != '\t') {
        out.push_back((char)c);
        c = gc();
    }
    return true;
}
static bool readLL(long long &v) {
    std::string s;
    if (!token(s)) return false;
    v = strtoll(s.c_str(), nullptr, 10);
    return true;
}
static bool readD(double &v) {
    std::string s;
    if (!token(s)) return false;
    v = strtod(s.c_str(), nullptr);
    return true;
}
}  // namespace io

// ---------------------------------------------------------------- globals

static int K, num_layers;

// The live request's position in its lifecycle. Exactly one of these is true at
// a time for the whole machine: NEED_* means a resource is free and the next
// task is issuable now, *_RUN means a task is running, and the UP_/DOWN_ states
// mean a transfer is in flight and nothing at all is issuable.
enum St {
    IDLE = 0,      // no live request
    NEED_PPRE, PPRE_RUN, UP_PRE, NEED_PPROC, PPROC_RUN, DOWN_PRE,
    NEED_PPOST, PPOST_RUN,
    NEED_DPRE, DPRE_RUN, UP_DEC, NEED_DPROC, DPROC_RUN, DOWN_DEC,
    NEED_DPOST, DPOST_RUN
};

int main() {
    std::string tok;

    {   // startup configuration
        long long k, npt, nl;
        double d;
        if (!io::readLL(k)) return 0;
        K = (int)k;
        io::readD(d);            // S
        io::readD(d);            // latency_in_ms
        io::readD(d);            // bandwidth_gbps
        io::readLL(npt);         // bytes_per_token
        io::readLL(nl); num_layers = (int)nl;
        for (int i = 0; i < 7; i++) io::readD(d);   // the scoring line
    }
    {   // task-time table: read in full, used by nobody. The serialized
        // reference never chooses between alternatives, so no cost matters.
        long long N;
        if (!io::readLL(N)) return 0;
        for (long long i = 0; i < N; i++) {
            double v;
            for (int c = 0; c < 7; c++)
                if (!io::readD(v)) return 0;
        }
    }

    std::deque<int> queued;      // arrived, not yet served, in arrival order
    int active = -1;             // the one request currently being served
    int remote = 0;              // its remote (always 0: everything is idle)
    St st = IDLE;

    char line[128];

    while (true) {
        if (!io::token(tok)) return 0;
        if (tok == "END") return 0;
        // tok is the frame timestamp; the reference never needs its value.
        long long e;
        if (!io::readLL(e)) return 0;

        // ---------------------------------------------------- read a frame
        for (long long i = 0; i < e; i++) {
            if (!io::token(tok)) return 0;
            if (tok == "ARR") {
                long long rid, Lin;
                io::readLL(rid); io::readLL(Lin);
                queued.push_back((int)rid);
            } else if (tok == "FIN") {
                long long rid;
                io::readLL(rid);
                (void)rid;
                active = -1;
                st = IDLE;
            } else if (tok == "TDN") {
                std::string sv, a, b;
                io::token(sv); io::token(a); io::token(b);
                double dur;
                if (a == "P" && b == "PRE") {
                    long long rm, rid;
                    io::readLL(rm); io::readLL(rid); io::readD(dur);
                    st = UP_PRE;
                } else if (a == "P" && b == "PROC") {
                    long long ls, le, rm, rid;
                    io::readLL(ls); io::readLL(le); io::readLL(rm); io::readLL(rid);
                    io::readD(dur);
                    // One full piece, so the remote-to-local transfer is queued.
                    st = DOWN_PRE;
                } else if (a == "P" && b == "POST") {
                    long long rm, rid;
                    io::readLL(rm); io::readLL(rid); io::readD(dur);
                    st = NEED_DPRE;
                } else {
                    long long rm, m;
                    io::readLL(rm); io::readLL(m);
                    for (long long j = 0; j < m; j++) { long long v; io::readLL(v); }
                    io::readD(dur);
                    if (b == "PRE") st = UP_DEC;
                    else if (b == "PROC") st = DOWN_DEC;
                    else st = NEED_DPRE;   // D POST; a FIN in this frame overrides
                }
            } else if (tok == "XDN") {
                std::string dir, kind;
                long long rem, size, m;
                io::token(dir); io::readLL(rem); io::readLL(size);
                io::token(kind); io::readLL(m);
                for (long long j = 0; j < m; j++) { long long v; io::readLL(v); }
                bool up = (dir == "UP");
                if (kind == "PRE") st = up ? NEED_PPROC : NEED_PPOST;
                else               st = up ? NEED_DPROC : NEED_DPOST;
            } else {
                return 0;  // unexpected token: bail out quietly
            }
        }

        // ------------------------------------------------------- decide
        //
        // FIN is delivered beside the final D POST's TDN, which frees the local
        // computer in the same frame, so the next request's P PRE starts
        // immediately: the serial makespan has no gap beyond waiting for
        // arrivals.
        if (st == IDLE && !queued.empty()) {
            active = queued.front();
            queued.pop_front();
            remote = 0;
            st = NEED_PPRE;
        }

        line[0] = 0;
        switch (st) {
            case NEED_PPRE:
                snprintf(line, sizeof(line), "E P PRE %d %d\n", remote, active);
                st = PPRE_RUN;
                break;
            case NEED_PPROC:
                snprintf(line, sizeof(line), "C%d P PROC 0 %d %d %d\n",
                         remote, num_layers, remote, active);
                st = PPROC_RUN;
                break;
            case NEED_PPOST:
                snprintf(line, sizeof(line), "E P POST %d %d\n", remote, active);
                st = PPOST_RUN;
                break;
            case NEED_DPRE:
                snprintf(line, sizeof(line), "E D PRE -1 1 %d\n", active);
                st = DPRE_RUN;
                break;
            case NEED_DPROC:
                snprintf(line, sizeof(line), "C%d D PROC %d 1 %d\n",
                         remote, remote, active);
                st = DPROC_RUN;
                break;
            case NEED_DPOST:
                snprintf(line, sizeof(line), "E D POST -1 1 %d\n", active);
                st = DPOST_RUN;
                break;
            default:
                break;   // a task or a transfer is in flight, or nothing is live
        }

        if (line[0]) printf("1\n%s", line);
        else printf("0\n");
        fflush(stdout);
    }
}
