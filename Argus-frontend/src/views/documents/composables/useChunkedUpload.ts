import { reactive, ref } from 'vue'
import {
  completeDocumentUpload,
  initDocumentUpload,
  uploadDocumentChunk,
} from '@/api/document'
import { extractApiError } from '@/api/http'

const CHUNK_SIZE = 5 * 1024 * 1024
const MAX_CONCURRENT_CHUNKS = 3

export type ChunkedUploadStage = 'hashing' | 'init' | 'uploading' | 'completing' | ''

async function sha256Hex(data: ArrayBuffer): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', data)
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('')
}

export function useChunkedUpload() {
  const isUploading = ref(false)
  const progress = ref(0)
  const error = ref('')
  const stage = ref<ChunkedUploadStage>('')
  const hashingProgress = ref('')
  const chunkProgress = reactive({ uploaded: 0, total: 0 })

  let abortController: AbortController | null = null

  function reset() {
    abortController = null
    isUploading.value = false
    progress.value = 0
    error.value = ''
    stage.value = ''
    hashingProgress.value = ''
    chunkProgress.uploaded = 0
    chunkProgress.total = 0
  }

  function cancel() {
    abortController?.abort()
  }

  async function uploadFile(file: File, groupId: number): Promise<void> {
    if (isUploading.value) {
      return
    }
    reset()
    isUploading.value = true
    const controller = new AbortController()
    abortController = controller
    const { signal } = controller

    try {
      const chunkCount = Math.max(1, Math.ceil(file.size / CHUNK_SIZE))

      stage.value = 'hashing'
      hashingProgress.value = '正在计算文件指纹...'
      const fileHash = await sha256Hex(await file.arrayBuffer())
      hashingProgress.value = ''

      stage.value = 'init'
      const initResult = await initDocumentUpload(
        {
          groupId,
          fileName: file.name,
          fileSize: file.size,
          contentType: file.type || 'application/octet-stream',
          fileHash,
          chunkSize: CHUNK_SIZE,
          chunkCount,
        },
        signal,
      )

      if (initResult.instantUpload) {
        progress.value = 100
        return
      }
      if (!initResult.uploadId) {
        throw new Error('初始化上传会话失败')
      }
      const uploadId = initResult.uploadId
      const chunkSize = initResult.chunkSize ?? CHUNK_SIZE
      const totalChunks = initResult.chunkCount ?? chunkCount
      chunkProgress.total = totalChunks

      const uploadedIndexes = new Set(initResult.uploadedChunks)
      const pendingIndexes: number[] = []
      let completedBytes = 0
      for (let index = 0; index < totalChunks; index += 1) {
        if (uploadedIndexes.has(index)) {
          completedBytes += chunkLength(file.size, chunkSize, index)
        } else {
          pendingIndexes.push(index)
        }
      }
      chunkProgress.uploaded = uploadedIndexes.size

      const inFlightBytes = new Map<number, number>()
      function updateProgress() {
        if (file.size <= 0) {
          progress.value = 100
          return
        }
        let loadedBytes = completedBytes
        for (const bytes of inFlightBytes.values()) {
          loadedBytes += bytes
        }
        progress.value = Math.min(99, Math.floor((loadedBytes / file.size) * 100))
      }
      updateProgress()

      stage.value = 'uploading'
      let nextIndex = 0
      async function uploadWorker() {
        while (nextIndex < pendingIndexes.length) {
          const chunkIndex = pendingIndexes[nextIndex]
          nextIndex += 1
          if (chunkIndex === undefined) {
            return
          }
          const start = chunkIndex * chunkSize
          const blob = file.slice(start, Math.min(start + chunkSize, file.size))
          const chunkHash = await sha256Hex(await blob.arrayBuffer())
          if (signal.aborted) {
            return
          }
          inFlightBytes.set(chunkIndex, 0)
          await uploadDocumentChunk(
            { uploadId, chunkIndex, chunkHash, chunk: blob },
            (loadedBytes) => {
              inFlightBytes.set(chunkIndex, loadedBytes)
              updateProgress()
            },
            signal,
          )
          inFlightBytes.delete(chunkIndex)
          completedBytes += blob.size
          chunkProgress.uploaded += 1
          updateProgress()
        }
      }

      const workerCount = Math.min(MAX_CONCURRENT_CHUNKS, pendingIndexes.length)
      await Promise.all(Array.from({ length: workerCount }, () => uploadWorker()))

      stage.value = 'completing'
      await completeDocumentUpload(uploadId, signal)
      progress.value = 100
    } catch (err) {
      error.value = signal.aborted ? '上传已取消' : extractApiError(err, '上传文件失败')
      throw err
    } finally {
      stage.value = ''
      if (abortController === controller) {
        abortController = null
      }
      isUploading.value = false
    }
  }

  return {
    isUploading,
    progress,
    error,
    stage,
    hashingProgress,
    chunkProgress,
    uploadFile,
    cancel,
    reset,
  }
}

function chunkLength(fileSize: number, chunkSize: number, chunkIndex: number): number {
  return Math.min(chunkSize, fileSize - chunkIndex * chunkSize)
}
