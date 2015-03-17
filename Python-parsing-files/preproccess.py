import os

def preprocess():
    path = os.getcwd()
    f = open(path+'/edges_new.txt','r')
    all_lines = f.readlines()
    node = {}
    count = 1
    for each_line in all_lines:
        print count
        count+=1 
        each_line = each_line.strip().split(' ')
        each_line = int(each_line[0])
        try:
            old_val = node[each_line]
            node[each_line] = old_val+1
        except:
            node[each_line] = 1
        
    f.close()
    
    f = open(path+'/edges_new.txt','r')
    fw = open(path+'/edges_withOut.txt','w')
    all_lines = f.readlines()
    
    defaultPR = float(1)/float(685230)
    count = 1
    for each_line in all_lines:
        print count
        count+=1
        each_line = each_line.strip().split(' ')
        startNode = int(each_line[0])
        to_write = str(each_line[0])+"\t"+str(each_line[1])+" "+str(node[startNode])+" "+str(defaultPR)+'\n'
        fw.write(to_write)
        
    fw.close()
    f.close()
        
preprocess()
              
