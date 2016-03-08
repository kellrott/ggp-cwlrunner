#!/usr/bin/env python

import argparse
import cwltool.draft2tool
import cwltool.workflow
import cwltool.main
from cwltool.process import shortname
import threading
import cwltool.docker
import fnmatch
import logging
import re
import os
import sys
from urlparse import urlparse

from gcloud import storage
from oauth2client.client import GoogleCredentials
from apiclient.discovery import build

from cwltool.process import get_feature

logging.basicConfig()

logger = logging
#logger = logging.getLogger('google.cwl-runner')
#logger.setLevel(logging.INFO)


class BucketFsAccess(cwltool.process.StdFsAccess):
    def __init__(self, basedir):
        self.collections = {}
        self.basedir = basedir

    def get_collection(self, path):
        p = path.split("/")
        if p[0].startswith("keep:") and arvados.util.keep_locator_pattern.match(p[0][5:]):
            pdh = p[0][5:]
            if pdh not in self.collections:
                self.collections[pdh] = arvados.collection.CollectionReader(pdh)
            return (self.collections[pdh], "/".join(p[1:]))
        else:
            return (None, path)

    def _match(self, collection, patternsegments, parent):
        if not patternsegments:
            return []

        if not isinstance(collection, arvados.collection.RichCollectionBase):
            return []

        ret = []
        # iterate over the files and subcollections in 'collection'
        for filename in collection:
            if patternsegments[0] == '.':
                # Pattern contains something like "./foo" so just shift
                # past the "./"
                ret.extend(self._match(collection, patternsegments[1:], parent))
            elif fnmatch.fnmatch(filename, patternsegments[0]):
                cur = os.path.join(parent, filename)
                if len(patternsegments) == 1:
                    ret.append(cur)
                else:
                    ret.extend(self._match(collection[filename], patternsegments[1:], cur))
        return ret

    def glob(self, pattern):
        collection, rest = self.get_collection(pattern)
        patternsegments = rest.split("/")
        return self._match(collection, patternsegments, "keep:" + collection.manifest_locator())

    def open(self, fn, mode):
        collection, rest = self.get_collection(fn)
        if collection:
            return collection.open(rest, mode)
        else:
            return open(self._abs(fn), mode)

    def exists(self, fn):
        collection, rest = self.get_collection(fn)
        if collection:
            return collection.exists(rest)
        else:
            return os.path.exists(self._abs(fn))

class GGPJob(object):
    def __init__(self, runner):
        self.ggprunner = runner
        self.running = False

    def run(self, dry_run=False, pull_image=True, **kwargs):
        print "Running", self.command_line
        for a in self.command_line:
            print type(a)
        print "args", kwargs
        for t in self.generatefiles:
            print "File", t

        #self.done({})
        """
        script_parameters = {
            "command": self.command_line
        }
        runtime_constraints = {}

        if self.generatefiles:
            vwd = arvados.collection.Collection()
            script_parameters["task.vwd"] = {}
            for t in self.generatefiles:
                if isinstance(self.generatefiles[t], dict):
                    src, rest = self.arvrunner.fs_access.get_collection(self.generatefiles[t]["path"].replace("$(task.keep)/", "keep:"))
                    vwd.copy(rest, t, source_collection=src)
                else:
                    with vwd.open(t, "w") as f:
                        f.write(self.generatefiles[t])
            vwd.save_new()
            for t in self.generatefiles:
                script_parameters["task.vwd"][t] = "$(task.keep)/%s/%s" % (vwd.portable_data_hash(), t)

        script_parameters["task.env"] = {"TMPDIR": "$(task.tmpdir)"}
        if self.environment:
            script_parameters["task.env"].update(self.environment)

        if self.stdin:
            script_parameters["task.stdin"] = self.pathmapper.mapper(self.stdin)[1]

        if self.stdout:
            script_parameters["task.stdout"] = self.stdout

        (docker_req, docker_is_req) = get_feature(self, "DockerRequirement")
        if docker_req and kwargs.get("use_container") is not False:
            print "docker request", docker_req
            #runtime_constraints["docker_image"] = arv_docker_get_image(self.arvrunner.api, docker_req, pull_image)

        try:
            print "Running", kwargs

            #logger.info("Job %s (%s) is %s", self.name, response["uuid"], response["state"])
            #if response["state"] in ("Complete", "Failed", "Cancelled"):
            #    self.done(response)
        except Exception as e:
            logger.error("Got error %s" % str(e))
            self.output_callback({}, "permanentFail")
        """
    
    """
    def update_pipeline_component(self, record):
        print "Updating components", record
        
    def done(self, record):
        try:
            self.update_pipeline_component(record)
        except:
            pass

        try:
            if record["state"] == "Complete":
                processStatus = "success"
            else:
                processStatus = "permanentFail"

            try:
                outputs = {}
                if record["output"]:
                    outputs = self.collect_outputs("keep:" + record["output"])
            except Exception as e:
                logger.exception("Got exception while collecting job outputs:")
                processStatus = "permanentFail"

            self.output_callback(outputs, processStatus)
        finally:
            #del self.arvrunner.jobs[record["uuid"]]
            pass
    """
    
class BucketPathMapper(cwltool.pathmapper.PathMapper):
    def __init__(self, bucket, referenced_files, basedir, **kwargs):
        print "pathmapper", referenced_files, basedir, kwargs
        self._pathmap = {}
        
        for s in referenced_files:
            u = urlparse(s)
            if u.scheme == 'file':
                print "Uploading", u.path
                blob = bucket.blob(os.path.basename(u.path))
                with open(u.path) as handle:
                    blob.upload_from_file(handle)
                self._pathmap[s] = (blob.path, os.path.basename(blob.path))
                print "Uploaded", blob.path


class GGPCommandTool(cwltool.draft2tool.CommandLineTool):
    def __init__(self, ggprunner, toolpath_object, **kwargs):
        super(GGPCommandTool, self).__init__(toolpath_object, outdir="$(task.outdir)", tmpdir="$(task.tmpdir)", **kwargs)
        self.ggprunner = ggprunner

    def makeJobRunner(self):
        return GGPJob(self.ggprunner)

    def makePathMapper(self, reffiles, input_basedir, **kwargs):
        print "make path mapper", kwargs
        return BucketPathMapper(self.ggprunner.bucket, reffiles, input_basedir, **kwargs)


class GGPCwlRunner(object):
    def __init__(self):
        self.jobs = {}
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)
        self.final_output = None
        self.uploaded = {}
        self.num_retries = 4

    def ggp_maketool(self, toolpath_object, **kwargs):
        if "class" in toolpath_object and toolpath_object["class"] == "CommandLineTool":
            return GGPCommandTool(self, toolpath_object, **kwargs)
        else:
            return cwltool.workflow.defaultMakeTool(toolpath_object, **kwargs)

    def output_callback(self, out, processStatus):
        if processStatus == "success":
            logger.info("Overall job status is %s", processStatus)
        else:
            logger.warn("Overall job status is %s", processStatus)

        self.final_output = out


    def on_message(self, event):
        if "object_uuid" in event:
                if event["object_uuid"] in self.jobs and event["event_type"] == "update":
                    if event["properties"]["new_attributes"]["state"] == "Running" and self.jobs[event["object_uuid"]].running is False:
                        uuid = event["object_uuid"]
                        with self.lock:
                            j = self.jobs[uuid]
                            logger.info("Job %s (%s) is Running", j.name, uuid)
                            j.running = True
                            j.update_pipeline_component(event["properties"]["new_attributes"])
                    elif event["properties"]["new_attributes"]["state"] in ("Complete", "Failed", "Cancelled"):
                        uuid = event["object_uuid"]
                        try:
                            self.cond.acquire()
                            j = self.jobs[uuid]
                            logger.info("Job %s (%s) is %s", j.name, uuid, event["properties"]["new_attributes"]["state"])
                            j.done(event["properties"]["new_attributes"])
                            self.cond.notify()
                        finally:
                            self.cond.release()

    def get_uploaded(self):
        return self.uploaded.copy()

    def add_uploaded(self, src, pair):
        self.uploaded[src] = pair

    def ggp_executor(self, tool, job_order, input_basedir, args, **kwargs):
        
        print tool, job_order, input_basedir, args
        print args.bucket

        client = storage.Client(project=args.project)
        bucket = client.get_bucket(args.bucket)

        self.client = client
        self.bucket = bucket
        jobiter = tool.job(job_order,
                        input_basedir,
                        self.output_callback,
                        **kwargs)

        for runnable in jobiter:
            if runnable:
                with self.lock:
                    runnable.run(**kwargs)
            else:
                if self.jobs:
                    try:
                        self.cond.acquire()
                        self.cond.wait()
                    finally:
                        self.cond.release()
                else:
                    logger.error("Workflow cannot make any more progress.")
                    break

        while self.jobs:
            try:
                self.cond.acquire()
                self.cond.wait()
            finally:
                self.cond.release()


        if self.final_output is None:
            raise cwltool.workflow.WorkflowException("Workflow did not return a result.")

        return self.final_output


def main(args, stdout, stderr):
    runner = GGPCwlRunner()
    args.insert(0, "--leave-outputs")
    parser = cwltool.main.arg_parser()
    parser.add_argument("--project", default=None)
    parser.add_argument("--bucket", default=None)

    return cwltool.main.main(args, executor=runner.ggp_executor, makeTool=runner.ggp_maketool, parser=parser)


if __name__ == "__main__":
    main(sys.argv[1:], sys.stdout, sys.stderr)