const k8s = require("@kubernetes/client-node");
const { stdout } = require("process");
const logger = require("pino")();
const stream = require("stream");
const R = require("ramda");
const { resolve } = require("path");
// const app = require("express")();
logger.level = "debug";

const NAMESPACE = process.env.NS || "default";

const kc = new k8s.KubeConfig();
// kc.loadFromCluster();
kc.loadFromFile("/Users/orlandobrea/.kube/config.pacs-cluster-hpn");

const k8sApi = kc.makeApiClient(k8s.CoreV1Api);
const exec = new k8s.Exec(kc);

function getStreamsForPod(podName) {
  const streamOut = new stream.PassThrough();
  const streamErr = new stream.PassThrough();
  streamOut.on("data", (chunk) => {
    // logger.debug({response: chunk.toString()}, podName.container);
    // logger.debug(chunk.toString(), podName.container);
  });
  streamErr.on("data", (chunk) => {
    // logger.error(chunk.toString(), podName.container);
  });
  return {
    out: streamOut,
    err: streamErr,
  };
}

function getPods(ns) {
  return new Promise((resolve, reject) => {
    k8sApi
      .listNamespacedPod(ns)
      .then((result) => resolve(result.body))
      .catch(reject);
  });
}

function getKindFromPod(pod) {
  return pod?.metadata?.ownerReferences[0]?.kind;
}

function isRequiredKind(kind) {
  const validKinds = ["statefulset", "deployment", "replicaset"];
  return validKinds.includes(kind.toLowerCase());
}

function filterByIncludedTypes(pods) {
  return pods.filter((pod) => R.pipe(getKindFromPod, isRequiredKind)(pod));
}

// [{container: appName, pod: Name, volumes: [name: xx, claimName: yyyy]}, ...]
function getPodsData(filter = null) {
  return function (pods) {
    const podsToCheck = filter ? filter(pods) : pods;
    const onlyWithPVC = (volume) => volume.persistentVolumeClaim;
    const extractNameAndClaimName = (volume) => ({
      name: volume.name,
      claimName: volume.persistentVolumeClaim.claimName,
    });

    return podsToCheck.map((pod) => ({
      container:
        pod.metadata.labels.app ??
        pod.metadata.labels["app.kubernetes.io/name"],
      pod: pod.metadata.name,
      volumes: pod.spec.volumes
        .filter(onlyWithPVC)
        .map(extractNameAndClaimName),
    }));
  };
}

function getPodsNamesFromNS(ns) {
  const filteredPodNames = getPodsData(filterByIncludedTypes);
  return new Promise((resolve, reject) => {
    getPods(ns)
      .then((pods) => {
        const names = filteredPodNames(pods.items);
        resolve(names);
      })
      .catch(reject);
  });
}

function getPVCNamesFromNS(ns) {
  return new Promise((resolve, reject) => {
    k8sApi
      .listNamespacedPersistentVolumeClaim(ns)
      .then((result) => resolve(result.body.items))
      .catch(reject);
  });
}

function execInSequence(execFn, outFn) {
  return function (podNames) {
    return podNames.reduce(
      (acc, current) => acc.then(() => execFn(current).then(outFn)),
      Promise.resolve(null)
    );
  };
}

const execCommandInPod = (cmd, ns) => async (podName) => {
  const stream = getStreamsForPod(podName);
  await exec.exec(
    ns,
    podName.pod,
    podName.container,
    cmd,
    stream.out,
    stream.err,
    stream.out,
    true
  );
};



const app = () => {

  getPVCNamesFromNS(NAMESPACE).then((names) => {
    names.map(name => logger.debug(name.metadata.name))
    getPodsNamesFromNS(NAMESPACE).then((names) => {
      const cmdToExecute = execCommandInPod(["df", "-h"], NAMESPACE);
      const logOutput = (rta) => logger.debug(rta);
      execInSequence(cmdToExecute, logOutput)(names);
    });
  })
}

app()

// app.get("/", (req, res) => {
// k8sApi
//   .listNamespacedPod(NAMESPACE)
//   .then((k8Res) => {
//     logger.debug(k8Res.body.items[0].metadata.name)
//     // console.log(k8Res.body);
//     // res.json(k8Res.body);
//   })
//   .catch((e) => {
//     logger.error(e)
//     // console.log(e);
//     // res.json(e);
//   });
// });

// app.listen(3000, () => {
// console.log("App corriendo");
// });
