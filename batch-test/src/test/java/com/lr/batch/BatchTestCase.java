package com.lr.batch;

import com.lr.source.beaver.bounded.BatchMain;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;

/**
 * @author xu.shijie
 * @since 2022/2/10
 */
public class BatchTestCase implements Serializable {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());


    @Test
    public void batch() throws Exception {
        PackagedProgram.Builder builder = PackagedProgram.newBuilder();
        builder.setJarFile(new File("target/batch-test-1.0-SNAPSHOT.jar"));
        builder.setEntryPointClassName(BatchMain.class.getName());

        Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS,  "192.168.104.201");
        configuration.setInteger(JobManagerOptions.PORT, 18081);
        configuration.setInteger(RestOptions.PORT,18081);

        RestClusterClient<String> client = new RestClusterClient<String>(configuration, "123");

        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(builder.build(),
                configuration, 1, true);
       client.submitJob(jobGraph).get();
    }
}
