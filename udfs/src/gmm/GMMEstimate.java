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

public class GMMEstimate extends EvalFunc<Tuple> {
    private TupleFactory tupleFactory = TupleFactory.getInstance();
    int dim;
    public GMMEstimate(String dim) {
	this.dim = Integer.valueOf(dim);
    }

    public Tuple exec(Tuple input) throws IOException {
	ArrayList<Double> mus = new ArrayList<Double>();
	ArrayList<Double> vars = new ArrayList<Double>();
	for(int d = 0; d < dim; d++) {
	    mus.add(0.0);
	    vars.add(0.0);
	}
	double sumProb = 0.0;
        try {
	    DataBag bag = (DataBag)input.get(0);
	    for(Tuple t: bag) {
		double prob = (Double)t.get(1);
		Tuple xs = (Tuple)t.get(2);
		sumProb += prob;
		for(int d = 0; d < dim; d++) {
		    mus.set(d, mus.get(d) + (Double)xs.get(d) * prob);
		}
	    }
	    for(int d = 0; d < dim; d++) {
		mus.set(d, mus.get(d) / sumProb);
	    }
	    for(Tuple t: bag) {
		double prob = (Double)t.get(1);
		Tuple xs = (Tuple)t.get(2);
		for(int d = 0; d < dim; d++) {
		    double z = (Double)xs.get(d) - mus.get(d);
		    vars.set(d, vars.get(d) + z * z * prob);
		}
	    }
	    for(int d = 0; d < dim; d++) {
		vars.set(d, vars.get(d) / sumProb);
	    }
	    Tuple out = tupleFactory.newTuple();
	    out.append(sumProb / bag.size());
	    for(int d = 0; d < dim; d++) {
		out.append(mus.get(d));
	    }
	    for(int d = 0; d < dim; d++) {
		out.append(Math.sqrt(vars.get(d)));
	    }
	    return out;
        } catch (Exception e) {
            // Throwing an exception will cause the task to fail.
            throw new IOException("Something bad happened!", e);
        }
    }
 }
