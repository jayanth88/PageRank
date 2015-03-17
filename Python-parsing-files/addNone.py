f = open('edges28.txt','r')
#num_of_nodes = int(685229)
lines = f.readlines()

havingOutEdge = set()

for each_line in lines:
    each_line = each_line.strip()
    each_line = each_line.split(' ')
    havingOutEdge.add(int(each_line[0]))
f.close()

noOutEdge = set()
for i in xrange(0, 685230):
    if not i in havingOutEdge:
        noOutEdge.add(i)

print str(685230 - len(havingOutEdge))+" ****"

fw = open('edges28.txt','a')
fw.write('\n')
count = 0

for each in noOutEdge:
    to_write = (str(each)+" <NONE>")
    if count < (len(noOutEdge)-1):
        to_write+='\n'
    count+=1
    fw.write(to_write)

fw.close()

