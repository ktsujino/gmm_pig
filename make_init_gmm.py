nmix = sum(1 for line in open('param/true_gmm.txt'))

ifp = open('data/data.txt')
ofp = open('data/init_gmm.txt', 'w')

for i in range(nmix):
    rand_sample = ifp.readline().rstrip().split("\t")
    row = []
    row.append(1.0/nmix) # mixture weight
    row += rand_sample
    row += [1.0] * len(rand_sample)
    ofp.write("\t".join([str(x) for x in row]) + "\n")
