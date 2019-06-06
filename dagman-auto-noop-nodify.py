#!/usr/bin/python

# JOB JobName SubmitDescriptionFileName [DIR directory] [NOOP] [DONE]
# PARENT ParentJobName... CHILD ChildJobName...

import os
import sys

class EdgeCounts:
    orig = 0
    opt = 0

def auto_noop_nodify(f, ec):
    for lineno,line in enumerate(f,1):
        uptokens = line.upper().split()
        if uptokens and uptokens[0] == 'PARENT':
            cidx = uptokens.index('CHILD')
            tokens = line.split()
            parents = tokens[1:cidx]
            children = tokens[cidx + 1:]
            edges = len(parents) * len(children)
            noop_edges = len(parents) + len(children)
            ec.orig += edges
            if noop_edges < edges:
                # add extra noop node if it reduces total edges
                noop = "autonoop.%d" % lineno
                yield "JOB %s noop.sub NOOP\n" % noop
                yield "PARENT %s CHILD %s\n" % (" ".join(parents), noop)
                yield "PARENT %s CHILD %s\n" % (noop, " ".join(children))
                ec.opt += noop_edges
            else:
                yield line
                ec.opt += edges
        else:
            yield line

def main(args):
    if len(args) != 2:
        print("Usage: %s in.dag out.dag" % os.path.basename(__file__))
        sys.exit(1)

    ec = EdgeCounts()
    inf = open(args[0])
    with open(args[1], "w") as outf:
        for line in auto_noop_nodify(inf, ec):
            outf.write(line)

    print("Original DAG (%s) has %s edges." % (args[0], format(ec.orig, ',')))
    print("Optimized DAG (%s) has %s edges." % (args[1], format(ec.opt, ',')))

if __name__ == '__main__':
    main(sys.argv[1:])

