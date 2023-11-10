import sys

fileName = sys.argv[1]

f = open(f"{fileName}.txt", 'r')
o = open(f"{fileName}-parsed.txt", 'w')

IGNORE = ['paxos_previous Dial() failed:', 'unexpected EOF', 'write unix ->',
          '2022/', 'reading body EOF']
lines = f.readlines()
for l in lines:
    ignore = False
    for i in IGNORE:
        if l.startswith(i):
            ignore = True
            break

    if not ignore:
        o.write(l)