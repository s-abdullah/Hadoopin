import java.io.*;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SocialRank {
	public static final float THRESHOLD = 0.001f;
	// CUSTOM TYPES FOr MR
	/////////////////////////////////////////////////////////////////////////////////////////////////
	public static class FloatAndText implements Writable {
	// class variables
  	Text friendlist = new Text();
  	FloatWritable rank = new FloatWritable();

  	//constructors
  	public FloatAndText() {}

  	public FloatAndText(float x, String y) {
  		this.rank.set(x);
  		this.friendlist.set(y);
  	}

  	//overridden fuctions from writable class
  	@Override
    public void readFields(DataInput in) throws IOException {
        rank.readFields(in);
        friendlist.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
    	rank.write(out);
    	friendlist.write(out);
    }

    @Override
    public String toString() {
        return this.rank + "\t" + this.friendlist;
    }

  	// functions
    public float getRank() {
      return this.rank.get();
    }

    public String getFriendList() {
      return this.friendlist.toString();
    }

    public void merge(FloatAndText other) {
    	String x1 = other.friendlist.toString();
    	String x2 = this.friendlist.toString();
    	this.friendlist.set(x2.concat(x1));

    	float y1 = other.rank.get();
    	float y2 = this.rank.get();
    	this.rank.set(y1+y2);

    }
  }


  public static class FloatCompare implements WritableComparable<FloatCompare> {
    // class variables
    FloatWritable value = new FloatWritable();

    // constructor
    public FloatCompare() {}
    public FloatCompare(float val) {
      this.value.set(val);
    }

    // functions
    public float getFC(){
      return this.value.get();
    }

    // overridden functions
    @Override
    public int compareTo(FloatCompare other) {

      if ( other.value.get() < this.value.get() ) {
          return -1;
      } else if ( other.value.get() > this.value.get() ) {
          return 1;
      } else {
          return 0;
      }
    
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      value.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      value.write(out);
    }
    
  }


  // MAP REDUCE FOR SORTIING AND FINAL OUTPUT
  /////////////////////////////////////////////////////////////////////////////////////////////////
  public static class CompareMapper
  	extends Mapper<Object, Text, FloatCompare, Text>{

    private Text name = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String val = value.toString();
      String[] lines = val.split("\n");
      float rank = 0.0f;

      for (String x : lines) {

        StringTokenizer itr = new StringTokenizer(x);

        while (itr.hasMoreTokens()) {
          name.set(itr.nextToken());
          rank = Float.parseFloat(itr.nextToken());
          context.write(new FloatCompare(rank), name);
          break;
        }     
      }
    }
  }
  public static class CompareReducer
  	extends Reducer<FloatCompare,Text,Text,Text> {

    public void reduce(FloatCompare key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text val : values) {
        context.write(val, new Text(String.valueOf(key.getFC())));
      }
    }
  }



  // MAP REDUCE FOR CONVERtiNG INPUT FILE
  /////////////////////////////////////////////////////////////////////////////////////////////////
  public static class ChangeMapper
  	extends Mapper<Object, Text, Text, Text>{
    
    private Text name = new Text();
    private Text friends = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        name.set(itr.nextToken());
        friends.set(itr.nextToken());
        context.write(name, friends);
      }
    }
  }
  public static class ChangeReducer
  	extends Reducer<Text,Text,Text,FloatAndText> {
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String friends = "";
      for (Text val : values) {
        friends = (val.toString()) + "\t" + friends;
      }
      context.write(key, new FloatAndText(1.0f, friends));
    }
  }


  // MAP REDUCE FOR RANK FIND
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public static class RankMapper
   extends Mapper<Object, Text, Text, FloatAndText>{
    private final static IntWritable one = new IntWritable(1);

    private Text name = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {


      String temp = "";
      String parse = "";
      float weight = 0.0f;
      String val = value.toString();
      String[] lines = val.split("\n");

      Text nme = new Text();

      for (String x : lines) {

        int counter = 0;

        StringTokenizer itr = new StringTokenizer(x);
          name.set(itr.nextToken());
          weight = Float.parseFloat(itr.nextToken());

        while (itr.hasMoreTokens()) {
          counter++;
          temp = itr.nextToken();
          // System.out.println("---!!---");
          // System.out.println(temp);
          // System.out.println("---!!---");

          parse = temp + "\t" + parse;
          // context.write(name, friend);
        }        
        // System.out.println(rank);
        float rank = (weight/counter);
        // System.out.println(rank);
        StringTokenizer itr2 = new StringTokenizer(parse);

        while (itr2.hasMoreTokens()) {
          // counter++;
          nme.set(itr2.nextToken());
          context.write(nme, new FloatAndText(rank, ""));
        }        
        context.write(name, new FloatAndText(0.0f, parse));

      }
    }
  }
  public static class RankReducer
       extends Reducer<Text,FloatAndText,Text,FloatAndText> {
    // private IntWritable result = new IntWritable();
    // private Text name = new Text();
    // float w = 1.0f;
    public void reduce(Text key, Iterable<FloatAndText> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // int sum = 0;
      float total = 0.0f;
      String friend = "";
      for (FloatAndText val : values) {
        total += val.getRank();
        friend = (val.getFriendList()) + "\t" + friend;
      }
      total = 0.15f + 0.85f*total;
      context.write(key, new FloatAndText(total, friend));
    }
  }

// 2-MAP REDUCE FOR DIFFERERNCE
/////////////////////////////////////////////////////////////////////////////////////////////////
  public static class DiffAllMapper
   extends Mapper<Object, Text, Text, Text>{
    private final static IntWritable one = new IntWritable(1);

    private Text name = new Text();
    private Text rank = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String val = value.toString();
      String[] lines = val.split("\n");

      Text nme = new Text();

      for (String x : lines) {

        StringTokenizer itr = new StringTokenizer(x);

        while (itr.hasMoreTokens()) {
          name.set(itr.nextToken());
          rank.set(itr.nextToken());
          context.write(name, rank);
          break;
        }     
      }
    }
  }
  public static class DiffAllReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float difference = 0.0f;
      float x = 0.0f;
      float y = 0.0f;
      for (Text val : values) {
        if (x == 0.0f) {
          x = Float.parseFloat(val.toString());
        } else {
          y = Float.parseFloat(val.toString());;
        }
      }
      difference = Math.abs(x - y);
      context.write(key, new Text(String.valueOf(difference)));
    }
  }

  public static class DiffMapper
   extends Mapper<Object, Text, Text, FloatAndText>{
    private final static IntWritable one = new IntWritable(1);

    private Text aller = new Text("all");
    // private Text rank = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      Text nme = new Text();
      Text name = new Text();

      float rnk = 0.0f;
      float rank = 0.0f;


      // Path filePath = ((FileSplit) context.getInputSplit()).getPath();
      // String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      // System.out.println(fileName);
      // System.out.println("---00---");
      StringTokenizer itr = new StringTokenizer(value.toString());
      String x = itr.nextToken();
      nme.set(x);
      name.set(x);
      rnk = Float.parseFloat(itr.nextToken());
      rank = rnk;
      // System.out.println(nme.toString());
      // System.out.println(rnk);      

      while (itr.hasMoreTokens()) {
        if (rnk > rank) {
          rank = rnk;
          name.set(nme.toString()); 
        }
        System.out.println(nme.toString());
        System.out.println(rnk);
        nme.set(itr.nextToken());
        rnk = Float.parseFloat(itr.nextToken());
      }     
      // System.out.println(rank);
      context.write(aller, new FloatAndText(rank, name.toString()));
    }
  }
  public static class DiffReducer
       extends Reducer<Text,FloatAndText,Text,Text> {
    public void reduce(Text key, Iterable<FloatAndText> values,
                       Context context
                       ) throws IOException, InterruptedException {


      float difference = 0.0f;
      Text name = new Text();
      float x = 0.0f;
      // float y = 0.0f;
      for (FloatAndText val : values) {
        // System.out.println(val.toString());
        // System.out.println("------");
        x = val.getRank();
        if (x > difference) {
          difference = x;
          name.set(val.getFriendList());
        } 
        // if (x == 0.0f) {
        //   x = Float.parseFloat(val.toString());
        // } else {
        //   y = Float.parseFloat(val.toString());
        //   x = 0.0f;
        // }
      }
      // difference = Math.abs(x - y);
      context.write(name, new Text(String.valueOf(difference)));
    }
  }

// Functions
////////////////////////////////////////////////////////////////////////////////////////////////
  	public static void initialize(String[] input) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Changer");
		job.setJarByClass(SocialRank.class);
		job.setMapperClass(ChangeMapper.class);
		job.setReducerClass(ChangeReducer.class);
		job.setNumReduceTasks(Integer.parseInt(input[3]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(input[2]))){
			// deleteing if he directory already exists
		   fs.delete(new Path(input[2]),true);
		}

		FileInputFormat.addInputPath(job, new Path(input[1]));			
		FileOutputFormat.setOutputPath(job, new Path(input[2]));
		
		job.waitForCompletion(true);
		// System.exit( ? 0 : 1);

  	}

  	public static void iterate(String[] input) throws Exception {

	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Ranker");
	    job.setJarByClass(SocialRank.class);
	    job.setMapperClass(RankMapper.class);
	    // job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(RankReducer.class);
	    job.setNumReduceTasks(Integer.parseInt(input[3]));
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatAndText.class);
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(new Path(input[2]))){
			// deleteing if he directory already exists
		   fs.delete(new Path(input[2]),true);
		}

	    FileInputFormat.addInputPath(job, new Path(input[1]));
	    FileOutputFormat.setOutputPath(job, new Path(input[2]));
	    
	    job.waitForCompletion(true);
	    // System.exit( ? 0 : 1);

  	}


  	public static void difference(String[] input) throws Exception {

	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "difference");
	    job.setJarByClass(SocialRank.class);
	    job.setMapperClass(DiffAllMapper.class);
	    // job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(DiffAllReducer.class);
	    job.setNumReduceTasks(Integer.parseInt(input[4]));
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    String newb = input[3] + "interim";
	    
	    FileSystem fs = FileSystem.get(conf);

		if(fs.exists(new Path(input[3]))){
			// deleteing if he directory already exists
		   fs.delete(new Path(input[3]),true);
		}
	    
	    MultipleInputs.addInputPath(job, new Path(input[1]),
	      TextInputFormat.class, DiffAllMapper.class);

	    MultipleInputs.addInputPath(job, new Path(input[2]),
	      TextInputFormat.class, DiffAllMapper.class);

	    FileOutputFormat.setOutputPath(job, new Path(newb));
	  
	    job.waitForCompletion(true);
	  
	    // starting second job
	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "single line output");
	    job2.setJarByClass(SocialRank.class);
	    job2.setMapperClass(DiffMapper.class);
	    // job.setCombinerClass(IntSumReducer.class);
	    job2.setReducerClass(DiffReducer.class);

	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(FloatAndText.class);

	    FileInputFormat.addInputPath(job2, new Path(newb));
	    FileOutputFormat.setOutputPath(job2, new Path(input[3]));

	    // System.exit(job2.waitForCompletion(true) ? 0 : 1);
	    // job.waitForCompletion(true);
		job2.waitForCompletion(true);
		if(fs.exists(new Path(newb))){
			// deleteing if he directory already exists
		   fs.delete(new Path(newb),true);
		}

  	}

  	public static void finish(String[] input) throws Exception {

	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "FINAL");
	    job.setJarByClass(SocialRank.class);
	    job.setMapperClass(CompareMapper.class);
	    // job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(CompareReducer.class);
	    job.setNumReduceTasks(Integer.parseInt(input[3]));
	    job.setOutputKeyClass(FloatCompare.class);
	    job.setOutputValueClass(Text.class);
	    FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(input[2]))) {
			// deleteing if he directory already exists
		   fs.delete(new Path(input[2]),true);
		}

	    FileInputFormat.addInputPath(job, new Path(input[1]));
	    FileOutputFormat.setOutputPath(job, new Path(input[2]));
	    job.waitForCompletion(true);
	    // System.exit(job.waitForCompletion(true) ? 0 : 1);

  	}



 // MAIN
/////////////////////////////////////////////////////////////////////////////////////////////////
	public static void main(String[] args) throws Exception {


		if (args.length == 0) {
			
			System.out.println("Illegal Input Format");
			System.out.println("Input Format: hadoop jar SocialRank.jar SocialRank *");
			System.out.println("1) init <input directory> <output directory> <#reducers>");
			System.out.println("2) iter <input directory> <output directory> <#reducers>");
			System.out.println("3) diff <input directory> <input directory> <output directory> <#reducers>");
			System.out.println("4) finish <input directory> <output directory> <#reducers>");
			System.out.println("5) composite <input directory> <output directory> <interim input1 directory> <interim input2 directory> <diff directory> <#reducers>");
			System.exit(0);

		} else if (args[0].equals("init")) {
			
			if (args.length != 4) {
				System.out.println("Illegal Input Format");
				System.exit(0);
			}

			System.out.println("init function initiating...");
				initialize(args);
			System.exit(0);
		
		} else if (args[0].equals("iter")) {
		
		
			if (args.length != 4) {
				System.out.println("Illegal Input Format");
				System.exit(0);
		
			}
		
			System.out.println("iter function initiating...");
				iterate(args);
			System.exit(0);
		
		} else if (args[0].equals("diff")) {
		
			if (args.length != 5) {
		
				System.out.println("Illegal Input Format");
				System.exit(0);
			}
		
			System.out.println("diff function initiating...");
				difference(args);
			System.exit(0);

		} else if (args[0].equals("finish")) {
		
		
			if (args.length != 4) {
		
				System.out.println("Illegal Input Format");
				System.exit(0);
		
			}
		
			System.out.println("finish function initiating...");
				finish(args);
			System.exit(0);

		} else if (args[0].equals("composite")) {
		
			if (args.length != 7) {
		
				System.out.println("Illegal Input Format");
				System.exit(0);
		
			}
		
			System.out.println("Initiating EVERYTHING!");

			String[] init = { "init", args[1], args[3], args[6]};

			String[] int1 = {"iter", args[3], args[4], args[6]};
			String[] int2 = {"iter", args[4], args[3], args[6]};

			String[] diff = {"diff", args[3], args[4], args[5], args[6]};

			String[] fin = {"finish", args[3], args[2], args[6]};

			int condition = 1;

			initialize(init);

			while (condition == 1) {

				iterate(int1);
				iterate(int2);

				difference(diff);
				System.out.println("---------");
                try{
         //        		System.out.println("---------");
    					// System.out.println(args[5]);
				     //    File folder = new File(args[5]);
				     //    File[] listOfFiles = folder.listFiles();
					    // for (int i = 0; i < listOfFiles.length; i++) {
					    //   if (listOfFiles[i].isFile()) {
					    //     System.out.println("File " + listOfFiles[i].getName());
					    //   } else if (listOfFiles[i].isDirectory()) {
					    //     System.out.println("Directory " + listOfFiles[i].getName());
					    //   }
					    // }
				    	// System.exit(0);



                        FileSystem fs = FileSystem.get(new Configuration());

                		String fileName = args[5] + "/part-r-00000";
                        Path check = new Path(fileName);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(check)));
                        String line;
                        line=br.readLine();

                        String name = "";
                        float differ = 0.0f;

                        while (line != null){
                        	StringTokenizer itr = new StringTokenizer(line);
                        	while (itr.hasMoreTokens()) {
                        		name = itr.nextToken();
                        		differ = Float.parseFloat(itr.nextToken());

                        		// System.out.print("name: ");
                        		// System.out.println(name);

                        		// System.out.print("difference: ");
                        		// System.out.println(differ);
                        		// System.out.println("---------");
                        	}
	                        line=br.readLine();
                        }
                        if (differ < THRESHOLD) {
                        	condition = 0;
                        }

                } catch (Exception e){
                	System.out.println(e);
                	System.exit(0);
                }




			}

			finish(fin);
			System.out.println("SOCIAL RANK SUCESSFULLY COMPLETED");
			System.exit(0);



		} else {
		
			System.out.println("Invalid Input");
			System.exit(0);
		
		}

	}








}