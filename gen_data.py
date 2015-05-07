import random
NB_SAMPLES=100000

pi = []
mu = []
gamma = []

dim = 0
for line in open('param/true_gmm.txt'):
    a = [float(f) for f in line.split()]
    dim = len(a) / 2
    pi.append(a[0])
    mu.append(a[1:(dim+1)])
    gamma.append(a[(dim+1):(2*dim+1)])

fp = open('data/data.txt', 'w')

for s in range(NB_SAMPLES):
    sample = []
    r = random.random()
    cum = 0.0
    m = 0
    for m_tmp in range(len(pi)):
        cum += pi[m_tmp]
        if r < cum:
            m = m_tmp
            break
    for d in range(dim):
        g = random.gauss(mu[m][d], gamma[m][d])
        sample.append(g)
    fp.write("\t".join([str(x) for x in sample]) + "\n")
