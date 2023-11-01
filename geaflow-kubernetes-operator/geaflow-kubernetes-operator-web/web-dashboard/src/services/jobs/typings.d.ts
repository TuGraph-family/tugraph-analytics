// @ts-ignore
/* eslint-disable */

declare namespace API {

  type CurrentUser = {
    name?: string
    avatar?: string
  }

  type GeaflowJob = {
    apiVersion?: string
    kind?: string
    metadata?: {
      creationTimestamp?: string
      generation?: number
      name?: string
      namespace?: string
      resourceVersion?: number
      uid?: string
    }
    spec?: {
      clientSpec?: {
        resource?: {
          cpuCores?: number
          jvmOptions?: string
          memoryMb?: number
        }
      }
      containerSpec?: {
        containerNum?: number
        resource?: {
          cpuCores?: number
          jvmOptions?: string
          memoryMb?: number
        }
        workerNumPerContainer?: number
      }
      driverSpec?: {
        driverNum?: number
        resource?: {
          cpuCores?: number
          jvmOptions?: string
          memoryMb?: number
        }
      }
      engineJars?: [
        {
          md5?: string
          name?: string
          url?: string
        }
      ]
      entryClass?: string
      image?: string
      imagePullPolicy?: string
      masterSpec?: {
        resource?: {
          cpuCores?: number
          jvmOptions?: string
          memoryMb?: number
        }
      }
      serviceAccount?: string
      udfJars?: [
        {
          md5?: string
          name?: string
          url?: string
        }
      ]
      userSpec?: {
        additionalArgs?: {}
        metricConfig?: {}
        stateConfig?: {}
      }
    }
    status?: {
      clientState?: string
      jobUid?: number
      lastReconciledSpec?: string
      masterState?: string
      state?: string
      errorMessage?: string
    }
  }

  type ClusterOverview = {
    host?: string
    namespace?: string
    masterUrl?: string
    totalJobNum?: number
    jobStateNumMap?: Record<string, number | undefined>
  }

}
