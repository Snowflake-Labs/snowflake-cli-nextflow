from snowflakecli.nextflow.manager import NextflowManager
from util.mock_command_runner import MockCommandRunner


def test_nextflow_manager_run_async(mock_db):
    # Create a custom mock runner with expected test configuration
    test_config = {
        "snowflake.computePool": "test",
        "snowflake.workDirStage": "data_stage",
        "snowflake.stageMounts": "input:/data/input,output:/data/output",
        "snowflake.enableStageMountV2": "true",
    }

    manager = NextflowManager(
        project_dir=".",
        profile="test",
        id_generator=lambda: "abc1234",
        command_runner=MockCommandRunner(test_config),
        temp_file_generator=lambda suffix: f"/tmp/tmp1234{suffix}",
    )
    manager.run_async()

    executed_queries = mock_db.get_executed_queries()
    # Check that we have the expected number of queries
    assert len(executed_queries) == 3

    # Check that the PUT command uses the deterministic file name
    put_query = executed_queries[0]
    assert put_query.startswith("PUT file:///tmp/tmp1234.tar.gz @data_stage/abc1234")

    # Check that the query tag is set correctly
    query_tag = executed_queries[1]
    assert "alter session set query_tag" in query_tag
    assert '"NEXTFLOW_JOB_TYPE": "main"' in query_tag
    assert '"NEXTFLOW_RUN_ID": "abc1234"' in query_tag

    assert (
        executed_queries[2]
        == """
EXECUTE JOB SERVICE
IN COMPUTE POOL test
NAME = NXF_MAIN_abc1234
FROM SPECIFICATION $$
spec:
  containers:
  - command:
    - /bin/bash
    - -c
    - "\\n        mkdir -p /mnt/project\\n        cd /mnt/project\\n        tar -zxf\\
      \\ /mnt/workdir/tmp1234.tar.gz\\n\\n        nextflow run . -name abc1234 -ansi-log\\
      \\ False -profile test -work-dir /mnt/workdir -with-report /tmp/report.html -with-trace\\
      \\ /tmp/trace.txt -with-timeline /tmp/timeline.html\\n        cp /tmp/report.html\\
      \\ /mnt/workdir/report.html\\n        cp /tmp/trace.txt /mnt/workdir/trace.txt\\n\\
      \\        cp /tmp/timeline.html /mnt/workdir/timeline.html\\n        "
    name: nf-main
    volumeMounts:
    - mountPath: /data/input
      name: vol-1
    - mountPath: /data/output
      name: vol-2
    - mountPath: /mnt/workdir
      name: workdir
  volumes:
  - name: vol-1
    source: stage
    stageConfig:
      enableSymlink: true
      name: '@input'
  - name: vol-2
    source: stage
    stageConfig:
      enableSymlink: true
      name: '@output'
  - name: workdir
    source: stage
    stageConfig:
      enableSymlink: true
      name: '@data_stage/abc1234/'

$$
"""
    )
