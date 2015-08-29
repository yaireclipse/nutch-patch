package com.geektime.crawl;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import org.apache.nutch.api.resources.AbstractResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path(value = "/ccjob")
public class ContinuousCrawlerJobResource extends AbstractResource {

	private static final Logger LOG = LoggerFactory.getLogger(ContinuousCrawlerJobResource.class);
	
	  @POST
	  @Path(value = "/create")
	  @Consumes(MediaType.APPLICATION_JSON)
	  public String create(final ContinuousCrawlJobConfig ccJobConfig) {
	    try {
			return "" + ContinuousCrawlerJob.run(ccJobConfig.getArgs());
		} catch (Exception e) {
			LOG.error("error during " + ContinuousCrawlerJob.class.getSimpleName() + " creation", e );
			return e.toString();
		}
	  }
}
