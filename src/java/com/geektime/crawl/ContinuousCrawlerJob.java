package com.geektime.crawl;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.DbUpdaterJob;
import org.apache.nutch.crawl.GeneratorJob;
import org.apache.nutch.crawl.InjectorJob;
import org.apache.nutch.fetcher.FetcherJob;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParserJob;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ContinuousCrawlerJob {

	private static final Logger LOG = LoggerFactory.getLogger(ContinuousCrawlerJob.class);
	
	private static final String BATCH_ID = "-batchId";
	private static final String TOP_N = "-topN";
	private static final String CYCLES = "-cycles";
	
	private static List<Class<? extends NutchTool>> crawlerFlowJobClasses = buildJobsflow();
	private static Map<String, Object> defaultArgs = buildDefaultArgs();
	
	private Map<Class<? extends NutchTool>, List<String>> crawlerFlowJobArgs;

	private ContinuousCrawlerJob() {
		crawlerFlowJobArgs = Maps.newHashMap();
		crawlerFlowJobArgs.put(InjectorJob.class, Lists.newArrayList("urls"));
		crawlerFlowJobArgs.put(GeneratorJob.class, Lists.newArrayList(TOP_N, "80"));
		crawlerFlowJobArgs.put(FetcherJob.class, Lists.newArrayList());
		crawlerFlowJobArgs.put(ParserJob.class, Lists.newArrayList());
		crawlerFlowJobArgs.put(DbUpdaterJob.class, Lists.newArrayList());
		crawlerFlowJobArgs.put(IndexingJob.class, Lists.newArrayList());
	}

	@SuppressWarnings("unchecked")
	private static List<Class<? extends NutchTool>> buildJobsflow() {
		return Lists.<Class<? extends NutchTool>>newArrayList(
				GeneratorJob.class,
				FetcherJob.class,
				ParserJob.class,
				DbUpdaterJob.class,
				IndexingJob.class);
		
	}
	
	private static Map<String, Object> buildDefaultArgs() {
		final HashMap<String, Object> args = Maps.newHashMap();
		args.put("inject", false);
		args.put("stage", 0);
		args.put(TOP_N, "80");
		args.put("cycles", Integer.MAX_VALUE);
		return Collections.unmodifiableMap(args);
	}

	public static void main(String[] args) throws Exception {
		int res = run(args);
		System.exit(res);
	}
	
	public static int run(String[] args) throws Exception {
		
		final Map<String, Object> ccJobArgs = new HashMap<String, Object>();
		
		ccJobArgs.put("inject", false);
		
		for (int i = 0; i < args.length; i++) {
			if ("-inject".equals(args[i])) {
				ccJobArgs.put("inject", true);
			} else if ("-stage".equals(args[i])) {
				checkArgument(args.length > i + 1, "stage argument specified without value");
				int stage = 1;
				try {
					stage = Integer.parseInt( args[i + 1] ) - 1;
					ccJobArgs.put("stage", stage);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("argument '-stage' must be a number. Actual value: " + args[i + 1]);
				}
				if (stage < 0 || stage >= crawlerFlowJobClasses.size() ) {
					throw new IllegalArgumentException("argument '-stage' must be a number between 1 and " + crawlerFlowJobClasses.size()
														+ ". Actual value: " + args[i + 1]);
				}
			} else if (BATCH_ID.equals(args[i])) {
				ccJobArgs.put("externalBatchId", args[++i]);
			} else if (TOP_N.equals(args[i])) {
				ccJobArgs.put(TOP_N,args[++i]);
			} else if (CYCLES.equals(args[i])) {
				ccJobArgs.put("cycles", Integer.parseInt(args[++i]));
			}
		}
		
		return run(ccJobArgs);
	}
		
	public static int run(final Map<String, Object> args) throws Exception {
		final ContinuousCrawlerJob ccJob = new ContinuousCrawlerJob();
		return ccJob.runJobInstance(args);
	}
	
	private int runJobInstance(final Map<String, Object> args) throws Exception {
		
		int res = 0;
		
		if ( (boolean) getArgValue("inject", args) ) {
			res = runJob(InjectorJob.class);
			if (res != 0) {
				System.exit(res);
			}
		}
		
		
		int jobClassIndex = (int) getArgValue("stage", args);		
		final String externalBatchId = (String) args.get("externalBatchId");
		setNamedArgument(TOP_N, (String) getArgValue(TOP_N, args), GeneratorJob.class);
		int maxCycles = (int) getArgValue("cycles", args);
		
		String batchId = null;
		int currCycle = 1;
		while (res == 0 && currCycle <= maxCycles) {
			final Class<? extends NutchTool> jobClass = crawlerFlowJobClasses.get(jobClassIndex);
			batchId = determineBatchId(jobClassIndex, batchId, externalBatchId);
			LOG.info(String.format("starting crawl stage: %s, crawl cycle: %d", jobClass.getSimpleName(), currCycle));
			res = runJob(jobClass);
			if (res == 0) {
				jobClassIndex++;
				jobClassIndex = jobClassIndex % crawlerFlowJobClasses.size();
				if (jobClassIndex == 0 ) {
					currCycle++;
				}
			} else { // retry in 5 sec.
				LOG.error("job " + jobClass.getSimpleName() + " returned error code result: " + res + ". Retrying in 5 seconds...");
				Thread.sleep(5000);
				res = 0;
			}
		}
		return res;
	}

	private Object getArgValue(final String name, final Map<String, Object> args) {
		if (args.containsKey(name)) {
			return args.get(name);
		}
		return defaultArgs.get(name);
	}
	
	private String determineBatchId(final int jobClassIndex, String batchId, final String externalBatchId) {
		final Class<? extends NutchTool> jobClass = crawlerFlowJobClasses.get(jobClassIndex);
		if (GeneratorJob.class == jobClass) {
			// batchId == null actually indicates that this is the first cycle of this crawl invocation
			if (batchId == null && externalBatchId != null) {
				batchId = externalBatchId;
			} else {
				// generate batchId
				int randomSeed = Math.abs(new Random().nextInt());
				batchId = (System.currentTimeMillis() / 1000) + "-" + randomSeed;
			}
		} else {
			if (batchId == null) {
				checkArgument(externalBatchId != null, "batchId must be specified when invoking crawl from " + jobClass.getSimpleName() + "(" + jobClassIndex + ") stage.");
				batchId = externalBatchId;
			}
		}
		setBatchId(batchId, jobClass);
		return batchId;
	}

	private void setBatchId(String batchId, final Class<? extends NutchTool> jobClass) {
		List<String> jobArgsList = crawlerFlowJobArgs.get(jobClass);		
		boolean generateStage = ( GeneratorJob.class == jobClass );
		if ( ! generateStage) {
			// batchId should always be the first argument (and it's specified without preceding arg name)
			if (jobArgsList.isEmpty()) {
				jobArgsList.add(0, batchId);
			} else {
				jobArgsList.set(0, batchId); // replace batchId
			}
			return;
		}
		setNamedArgument(BATCH_ID, batchId, jobClass);
	}
	
	private void setNamedArgument(String name, String value, final Class<? extends NutchTool> jobClass) {
		List<String> jobArgsList = crawlerFlowJobArgs.get(jobClass);		
		for (int i = 0; i < jobArgsList.size(); i++) {
			String arg = jobArgsList.get(i);
			if (arg.equals(name)) {
				jobArgsList.set(i + 1, value);
				return;
			}
		}
		// if got here: value doesn't exist yet
		jobArgsList.add(name);
		jobArgsList.add(value);
	}

	private int runJob(final Class<? extends NutchTool> jobClass)
			throws InstantiationException, IllegalAccessException, Exception {
		List<String> jobArgsList = crawlerFlowJobArgs.get(jobClass);
		String[] jobArgs = new String[jobArgsList.size()];
		final NutchTool jobInstance = jobClass.newInstance();
		int res = ToolRunner.run(NutchConfiguration.create(), (Tool) jobInstance, jobArgsList.toArray(jobArgs));
		return res;
	}
	
}
