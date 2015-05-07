/**
 * This code is made available under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gmm;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;
import org.apache.pig.impl.util.UDFContext;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.lang.Math;
import java.util.ArrayList;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class GMMProb extends EvalFunc<Tuple> {
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    private BagFactory bagFactory = BagFactory.getInstance();
    String paramFile;
    int dim;
    ArrayList<Double> pis;
    ArrayList<ArrayList<Double>> muMatrix;
    ArrayList<ArrayList<Double>> gammaMatrix;
    
    public GMMProb(String file, String dim) {
	paramFile = file;
	this.dim = Integer.valueOf(dim);
    }

    public void load() throws IOException {
            FileSystem fs =
                FileSystem.get(UDFContext.getUDFContext().getJobConf());
            DataInputStream in = fs.open(new Path(paramFile));
            String line;
	    pis = new ArrayList<Double>();
	    muMatrix = new ArrayList<ArrayList<Double>>();
	    gammaMatrix = new ArrayList<ArrayList<Double>>();
            while ((line = in.readLine()) != null) {
		if(line.equals("")) {
		    continue;
		}
                String[] fields = line.split("\t");
		ArrayList<Double> mu = new ArrayList<Double>();
		ArrayList<Double> gamma = new ArrayList<Double>();
		pis.add(Double.valueOf(fields[0]));
		for(int d = 0; d < dim; d++) {
		    mu.add(Double.valueOf(fields[1+d]));
		    gamma.add(Double.valueOf(fields[1+dim+d]));
		}
		muMatrix.add(mu);
		gammaMatrix.add(gamma);
            }
            in.close();
    }

    public Tuple exec(Tuple input) throws IOException {
        try {
	    if(muMatrix == null) {
		load();
	    }
	    Tuple xs = (Tuple)input.get(0);
	    int dim = xs.size();
	    int nMix = muMatrix.size();
	    DataBag y = bagFactory.newDefaultBag();
	    double sumProb = 0.0;
	    for(int m = 0; m < nMix; m++) {
		double pi = (Double)pis.get(m);
		ArrayList<Double> mus = muMatrix.get(m);
		ArrayList<Double> gammas = gammaMatrix.get(m);
		double prob = 1.0;
		for(int d = 0; d < dim; d++) {
		    double mu = (Double)mus.get(d);
		    double gamma = (Double)gammas.get(d);
		    double x = (Double)xs.get(d);
		    double z = (x-mu) / (Math.sqrt(2.0) * gamma);
		    prob *= pi * Math.sqrt(1.0 / (2.0 * Math.PI)) / gamma * Math.exp(- z * z);
		}
		sumProb += prob;
		Tuple triple = tupleFactory.newTuple(3);
		triple.set(0, m);
		triple.set(1, (Double)prob);
		triple.set(2, xs);
		y.add(triple);
	    }
	    for(Tuple triple : y) {
		triple.set(1, (Double)((Double)triple.get(1) / sumProb));
	    }	    
	    Tuple out = tupleFactory.newTuple(2);
	    out.set(0, (Double)Math.log10(sumProb));
	    out.set(1, y);
	    return out;
        } catch (Exception e) {
            // Throwing an exception will cause the task to fail.
            throw new IOException("Something bad happened!", e);
        }
    }
    public Schema outputSchema(Schema input) {
	try {
        // Construct our output schema which is one field, that is a long
	    Schema tupleSchema = new Schema();
	    tupleSchema.add(new Schema.FieldSchema("m", DataType.INTEGER));
	    tupleSchema.add(new Schema.FieldSchema("nprob", DataType.DOUBLE));
	    tupleSchema.add(new Schema.FieldSchema("x", DataType.TUPLE));
	    Schema bagSchema = new Schema(new Schema.FieldSchema(null, tupleSchema, DataType.TUPLE));
	    Schema outerTupleSchema = new Schema();
	    outerTupleSchema.add(new Schema.FieldSchema("logprob", DataType.DOUBLE));
	    outerTupleSchema.add(new Schema.FieldSchema("y", bagSchema, DataType.BAG));
	    return new Schema(new Schema.FieldSchema(null, outerTupleSchema, DataType.TUPLE));
        } catch (Exception e) {
	    return null;
        }
    }
 }
