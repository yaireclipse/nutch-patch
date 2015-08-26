package com.geektime.crawl;

import static com.google.common.base.Preconditions.checkArgument;

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
	
	public static List<Class<? extends NutchTool>> crawlerFlowJobClasses;
	public static Map<Class<? extends NutchTool>, List<String>> crawlerFlowJobArgs;

	private static int cycles;
	
	static {
		crawlerFlowJobClasses = Lists.<Class<? extends NutchTool>>newArrayList(
				GeneratorJob.class,
				FetcherJob.class,
				ParserJob.class,
				DbUpdaterJob.class,
				IndexingJob.class);
		crawlerFlowJobArgs = Maps.newHashMap();
		crawlerFlowJobArgs.put(InjectorJob.class, Lists.newArrayList("urls"));
		crawlerFlowJobArgs.put(GeneratorJob.class, Lists.newArrayList(TOP_N, "80"));
		crawlerFlowJobArgs.put(FetcherJob.class, Lists.newArrayList());
		crawlerFlowJobArgs.put(ParserJob.class, Lists.newArrayList());
		crawlerFlowJobArgs.put(DbUpdaterJob.class, Lists.newArrayList());
		crawlerFlowJobArgs.put(IndexingJob.class, Lists.newArrayList());
	}
	
	public static void main(String[] args) throws Exception {
		
		int jobClassIndex = 0;
		int res = 0;
		
		boolean inject = false;
		String externalBatchId = null;
		
		for (int i = 0; i < args.length; i++) {
			if ("-inject".equals(args[i])) {
				inject = true;
			} else if ("-stage".equals(args[i])) {
				checkArgument(args.length > i + 1, "stage argument specified without value");
				try {
					jobClassIndex = Integer.parseInt( args[i + 1] ) - 1;
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("argument '-stage' must be a number. Actual value: " + args[i + 1]);
				}
				if (jobClassIndex < 0 || jobClassIndex >= crawlerFlowJobClasses.size() ) {
					throw new IllegalArgumentException("argument '-stage' must be a number between 1 and " + crawlerFlowJobClasses.size()
														+ ". Actual value: " + args[i]);
				}
			} else if (BATCH_ID.equals(args[i])) {
		        externalBatchId = args[++i];
			} else if (TOP_N.equals(args[i])) {
				setNamedArgument(TOP_N, args[++i], GeneratorJob.class);
			} else if (CYCLES.equals(args[i])) {
				cycles = Integer.parseInt(args[++i]);
			}
		}
		
		if (inject) {
			res = runJob(InjectorJob.class);
			if (res != 0) {
				System.exit(res);
			}
		}
		
		String batchId = null;
		int currCycle = 1;
		while (res == 0 && currCycle <= cycles) {
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
		System.exit(res);
	}

	private static String determineBatchId(int jobClassIndex, String batchId, String externalBatchId) {
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

	private static void setBatchId(String batchId, final Class<? extends NutchTool> jobClass) {
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
	
	private static void setNamedArgument(String name, String value, final Class<? extends NutchTool> jobClass) {
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

	private static int runJob(final Class<? extends NutchTool> jobClass)
			throws InstantiationException, IllegalAccessException, Exception {
		List<String> jobArgsList = crawlerFlowJobArgs.get(jobClass);
		String[] jobArgs = new String[jobArgsList.size()];
		final NutchTool jobInstance = jobClass.newInstance();
		int res = ToolRunner.run(NutchConfiguration.create(), (Tool) jobInstance, jobArgsList.toArray(jobArgs));
		return res;
	}
	
}
