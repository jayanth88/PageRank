import os

def maxNode():
    nodes = [10328, 20373, 30629, 40645, 50462, 60841, 70591,
        80118, 90497,100501, 110567, 120945, 130999, 140574, 150953, 161332, 171154,
        181514, 191625, 202004, 212383, 222762, 232593, 242878, 252938, 263149, 273210,
        283473, 293255, 303043, 313370, 323522, 333883, 343663, 353645, 363929, 374236,
        384554, 394929, 404712, 414617, 424747, 434707, 444489, 454285, 464398, 474196,
        484050, 493968, 503752, 514131, 524510, 534709, 545088, 555467, 565846, 576225,
        586604, 596585, 606367, 616148, 626448, 636240, 646022, 655804, 665666, 675448, 685230]
    
    path = os.getcwd()
    file_list = os.listdir(path+"/output")
    
    nodePR = {}
    
    for each_file in file_list:
        f = open(path+"/output/"+each_file,'r')
        all_lines = f.readlines()
        for each_line in all_lines:
            each_line = each_line.strip().split('\t')
            if nodes.__contains__(int(each_line[0])+1):
                pageRank = each_line[1].split(' ')
                pageRank = float(pageRank[2])
                nodePR[int(each_line[0])] = pageRank
        f.close()
        
    nodePR = sorted(nodePR.items())
    print len(nodePR)
    for node, rank in nodePR:
        print "Node: "+str(node)+" PR: "+str(rank)
        
        
    
    
maxNode()