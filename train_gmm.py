#!/usr/bin/python
import os
from org.apache.pig.scripting import *

UPDATE = Pig.compile("""
REGISTER './udfs/gmm.jar'
DEFINE GMMProb gmm.GMMProb('$gmm_in', '$gmm_dim');
DEFINE GMMEstimate gmm.GMMEstimate('$gmm_dim');

data = LOAD 'data/data.txt' as ($fields);
data = foreach data generate TOTUPLE(*) as x;
out = foreach data generate GMMProb(x) as probs;
describe out;
out_f = foreach out generate flatten(probs.y);
logprobs = foreach out generate probs.logprob as logprob;
logprobs_collected = group logprobs all;
avg_logprob = foreach logprobs_collected generate AVG(logprobs.logprob);
grp = group out_f by m;
describe grp;
est = foreach grp generate GMMEstimate(out_f);
est = foreach est generate flatten($0);
store est into '$gmm_out';
store avg_logprob into '$logprob';
""")

os.system("rm -rf data")
os.system("mkdir data")
os.system("python gen_data.py")
os.system("python make_init_gmm.py")

os.system("rm -rf out")
os.system("mkdir out")
os.system("cp data/init_gmm.txt out/gmm0.txt")

gmm_dim = len(open("data/init_gmm.txt").readline().split("\t"))/2
fields = ', '.join(['f' + str(i) + ':double' for i in range(gmm_dim)])

for i in range(10):
    gmm_in = "out/gmm" + str(i) + ".txt"
    gmm_out = "out/gmm" + str(i+1)
    logprob = "out/logprob" + str(i)
    os.system("rm -rf " + gmm_out)
    os.system("rm -rf " + logprob)
    bound = UPDATE.bind()
    stats = bound.runSingle()
    if not stats.isSuccessful():
        raise 'failed'
    os.system("cat " + gmm_out + "/part* > " + gmm_out + ".txt")
    os.system("cat " + logprob + "/part* > " + logprob + ".txt")
