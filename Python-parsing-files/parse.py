import re
import os

def parse():
    path = os.getcwd()
    f=open(path+'/edges_dem.txt','r')
    fw=open(path+'/edges28.txt','w')
    lines=f.readlines()
    fromNetID=0.82
    rejectMin=0.99 * fromNetID
    rejectLimit = rejectMin + 0.01
    print 'RejectMin: '+str(rejectMin)
    print 'RejectLimit: '+str(rejectLimit)
    for i in range(0,len(lines)):
        line=re.sub(' +',' ',lines[i]).strip()
        nodes=line.split(' ')
        line=nodes[0]+' '+nodes[1]
        probVal=float(nodes[2])
        if i < (len(lines)-1):
            line+='\n'
        if not (probVal>=rejectMin and probVal <rejectLimit):
            fw.write(line)
    fw.close()
    f.close()

parse()
