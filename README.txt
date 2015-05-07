# abstract
Sample apache Pig application of training GMM using EM algorithm.
This implementation is NOT practical with limitations below:
- Probabiliies are calculated in linear domain, NOT in log domain.
- Covariance matrix of Gaussian is assumed diagonal.

# files
|--README.txt : this file
|--clean.sh: removes all temporary and result files
|--gen_data.py: generates artificial data samples using GMM defined by param/true_gmm.txt
|--make_init_gmm.py: generates initial parameter for EM algorithm (data/init_gmm.txt) by treating first $nmix samples of data as mean vectors.
|--run.sh: Runs train_gmm.py in local mode of Pig.
|--train_gmm.py: Pig application (written in Python2 with a Pig script embedded).
|--param
    |--true_gmm.txt: Used by gen_data.py. Each line is tab-separated gaussian component. Fields: prob, mean[0],...,mean[N-1],stdiv[0],...,stdiv[N-1].
|--udfs
    |--build.xml : run 'ant -Dpig.dir=/usr/lib/pig/' to build gmm.jar.
    |--gmm.jar: Contains Java UDFs (User Defined Functions) called from Pig scripts.
    |--src
        |--GMMEstimate.java: estimates GMM parameters with EM algorithm
        |--GMMProb.java: calculates GMM probability

## algorithm overview

GMMProb loads GMM parameters from a text file given to its constructor.

GMMProb estimates prior probability of input sample (in log domain) and posterior probability for each Gaussian component.
Output schema of GMMProb is as follows:
(logprob:double, {(m: int, nprob:double, x:tuple())})
logprob is the probability density of current GMM at the position of the input sample.
m is the id of the gaussian component [0, nmix).
nprob is the posterior probability of Gaussian component m.
x is a copy of the input sample.

After GMMprob is applied to the input samples (MAP phase), the output is GROUPed by m as key. (SHUFFLE phase)
Each group of data is inputted to GMMEstimate and updated GMM parameters are calculated there. (REDUCE phase)

The procedure above is irerated in train_gmm.py. (EM algorithm)
The output is generated in out/ dir.
gmmN.txt : GMM parameters after iteration N (in the same format as true_gmm.txt)
logprobN.txt : Average (prior) log probability of each input samples at iteration N.

Please note that the estimated GMM does NOT always converges to true GMM parameters (true_gmm.txt).
This is due to the fact that EM algorithm can fall into local minimum, and that we use quite naive method to get initial parameters.
