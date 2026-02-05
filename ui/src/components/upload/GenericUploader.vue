<template>
  <div class="generic-uploader">
    <!-- Selection Mode Buttons -->
    <div class="selection-mode-buttons mb-4">
      <va-button-group>
        <va-button
          :color="selectionMode === 'files' ? 'primary' : 'secondary'"
          @click="selectFiles"
          :disabled="uploading"
        >
          <Icon icon="mdi:file-multiple" class="mr-2" />
          Select Files
        </va-button>
        
        <va-button
          v-if="allowDirectory"
          :color="selectionMode === 'directory' ? 'primary' : 'secondary'"
          @click="selectDirectory"
          :disabled="uploading"
        >
          <Icon icon="mdi:folder-open" class="mr-2" />
          Select Directory
        </va-button>
      </va-button-group>
    </div>
    
    <!-- Hidden file inputs -->
    <input
      ref="fileInput"
      type="file"
      :multiple="allowMultiple"
      style="display: none"
      @change="handleFileSelect"
    />
    
    <input
      v-if="allowDirectory"
      ref="directoryInput"
      type="file"
      webkitdirectory
      directory
      style="display: none"
      @change="handleDirectorySelect"
    />
    
    <!-- Selected Files Preview -->
    <div v-if="selectedFiles.length > 0" class="selected-files mb-4">
      <va-card>
        <va-card-title>
          Selected: {{ selectedFiles.length }} file(s)
          <span v-if="selectionMode === 'directory'">
            from {{ directoryName }}
          </span>
        </va-card-title>
        <va-card-content>
          <div class="file-list" style="max-height: 300px; overflow-y: auto">
            <div
              v-for="(file, index) in selectedFiles"
              :key="index"
              class="file-item flex justify-between items-center py-2"
            >
              <div class="flex-grow">
                <Icon :icon="getFileIcon(file)" class="mr-2" />
                {{ getRelativePath(file) }}
                <span class="text-gray-500 ml-2 text-sm">
                  ({{ formatFileSize(file.size) }})
                </span>
              </div>
              
              <!-- Progress bar per file -->
              <va-progress-bar
                v-if="fileProgress[index] !== undefined"
                :model-value="fileProgress[index]"
                style="width: 100px"
                class="ml-4"
              />
              
              <!-- Remove button -->
              <va-button
                v-if="!uploading"
                preset="plain"
                icon="mdi:close"
                size="small"
                @click="removeFile(index)"
              />
            </div>
          </div>
        </va-card-content>
      </va-card>
    </div>
    
    <!-- Overall Upload Progress -->
    <div v-if="uploading" class="upload-progress mb-4">
      <va-progress-bar
        :model-value="overallProgress"
        :color="uploadStatus === 'error' ? 'danger' : 'primary'"
      />
      <div class="text-center mt-2">
        {{ uploadedFiles }} / {{ totalFiles }} files uploaded
        ({{ overallProgress.toFixed(1) }}%)
      </div>
    </div>
    
    <!-- Upload Button -->
    <va-button
      v-if="selectedFiles.length > 0 && !uploading"
      color="primary"
      @click="startUpload"
      :disabled="!canUpload"
    >
      Upload {{ selectedFiles.length }} File(s)
    </va-button>
    
    <!-- Cancel Button -->
    <va-button
      v-if="uploading"
      color="danger"
      @click="cancelUpload"
      class="ml-2"
    >
      Cancel Upload
    </va-button>
    
    <!-- Status Messages -->
    <div v-if="uploadStatus === 'complete'" class="mt-4 text-success">
      ✓ Upload complete! All files uploaded successfully.
    </div>
    <div v-if="uploadStatus === 'error'" class="mt-4 text-danger">
      ✗ Upload failed. Please try again.
    </div>
  </div>
</template>

<script setup>
import config from '@/config'
import { Icon } from '@iconify/vue'
import * as tus from 'tus-js-client'
import { computed, ref } from 'vue'

const props = defineProps({
  entityType: {
    type: String,
    required: true  // 'dataset', 'session', etc.
  },
  entityId: {
    type: [String, Number],
    required: true
  },
  metadata: {
    type: Object,
    default: () => ({})
  },
  allowMultiple: {
    type: Boolean,
    default: true
  },
  allowDirectory: {
    type: Boolean,
    default: true
  }
})

const emit = defineEmits([
  'upload-complete',
  'upload-error',
  'upload-progress',
  'files-selected'
])

// Refs
const fileInput = ref(null)
const directoryInput = ref(null)
const selectedFiles = ref([])
const selectionMode = ref(null)
const directoryName = ref(null)

const uploading = ref(false)
const uploadStatus = ref(null)
const fileProgress = ref({})
const uploadedFiles = ref(0)
const activeUploads = ref([])

// Computed
const totalFiles = computed(() => selectedFiles.value.length)

const overallProgress = computed(() => {
  if (totalFiles.value === 0) return 0
  
  const totalProgress = Object.values(fileProgress.value).reduce(
    (sum, progress) => sum + (progress || 0),
    0
  )
  
  return totalProgress / totalFiles.value
})

const canUpload = computed(() => {
  return selectedFiles.value.length > 0 && !uploading.value
})

// Methods: File Selection
const selectFiles = () => {
  selectionMode.value = 'files'
  fileInput.value.click()
}

const selectDirectory = () => {
  selectionMode.value = 'directory'
  directoryInput.value.click()
}

const handleFileSelect = (event) => {
  const files = Array.from(event.target.files)
  selectedFiles.value = files
  directoryName.value = null
  
  emit('files-selected', { files, mode: 'files' })
}

const handleDirectorySelect = (event) => {
  const files = Array.from(event.target.files)
  selectedFiles.value = files
  
  // Extract directory name from first file path
  if (files.length > 0) {
    const firstFile = files[0]
    const pathParts = firstFile.webkitRelativePath.split('/')
    directoryName.value = pathParts[0]
  }
  
  emit('files-selected', { 
    files, 
    mode: 'directory', 
    directoryName: directoryName.value 
  })
}

const removeFile = (index) => {
  selectedFiles.value.splice(index, 1)
}

const getRelativePath = (file) => {
  if (file.webkitRelativePath) {
    return file.webkitRelativePath
  }
  return file.name
}

// Methods: Upload
const startUpload = async () => {
  uploading.value = true
  uploadStatus.value = 'uploading'
  uploadedFiles.value = 0
  fileProgress.value = {}
  activeUploads.value = []
  
  // Create upload for each file
  for (let i = 0; i < selectedFiles.value.length; i++) {
    const file = selectedFiles.value[i]
    
    fileProgress.value[i] = 0
    
    const upload = createUpload(file, i)
    activeUploads.value.push(upload)
    
    upload.start()
  }
}

const createUpload = (file, fileIndex) => {
  const relativePath = file.webkitRelativePath 
    ? file.webkitRelativePath.split('/').slice(1).join('/')
    : null
  
  const endpoint = `${config.apiBasePath}/uploads/files`
  
  return new tus.Upload(file, {
    endpoint,
    
    metadata: {
      filename: file.name,
      filetype: file.type,
      entity_type: props.entityType,
      entity_id: String(props.entityId),
      
      ...(relativePath && { relative_path: relativePath }),
      
      selection_mode: selectionMode.value,
      ...(directoryName.value && { directory_name: directoryName.value }),
      
      ...props.metadata
    },
    
    onProgress: (bytesUploaded, bytesTotal) => {
      const progress = (bytesUploaded / bytesTotal * 100)
      fileProgress.value[fileIndex] = progress
      
      emit('upload-progress', {
        fileIndex,
        filename: file.name,
        progress,
        bytesUploaded,
        bytesTotal,
        overallProgress: overallProgress.value
      })
    },
    
    onSuccess: () => {
      uploadedFiles.value++
      fileProgress.value[fileIndex] = 100
      
      if (uploadedFiles.value === totalFiles.value) {
        uploadStatus.value = 'complete'
        uploading.value = false
        
        emit('upload-complete', {
          files: selectedFiles.value,
          mode: selectionMode.value,
          directoryName: directoryName.value
        })
      }
    },
    
    onError: (error) => {
      console.error(`Upload failed for ${file.name}:`, error)
      uploadStatus.value = 'error'
      uploading.value = false
      
      emit('upload-error', {
        file,
        fileIndex,
        error
      })
    }
  })
}

const cancelUpload = () => {
  activeUploads.value.forEach(upload => {
    upload.abort()
  })
  
  uploading.value = false
  uploadStatus.value = 'cancelled'
  activeUploads.value = []
}

// Utility functions
const getFileIcon = (file) => {
  const ext = file.name.split('.').pop().toLowerCase()
  const iconMap = {
    pdf: 'mdi:file-pdf',
    doc: 'mdi:file-word',
    docx: 'mdi:file-word',
    txt: 'mdi:file-document',
    jpg: 'mdi:file-image',
    jpeg: 'mdi:file-image',
    png: 'mdi:file-image',
    gif: 'mdi:file-image',
    zip: 'mdi:folder-zip',
    tar: 'mdi:folder-zip',
    gz: 'mdi:folder-zip',
    js: 'mdi:language-javascript',
    py: 'mdi:language-python',
    json: 'mdi:code-json',
    bam: 'mdi:dna',
    fastq: 'mdi:dna',
    vcf: 'mdi:dna',
    bed: 'mdi:dna',
  }
  
  return iconMap[ext] || 'mdi:file'
}

const formatFileSize = (bytes) => {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i]
}
</script>

<style scoped>
.file-item {
  border-bottom: 1px solid #eee;
}

.file-item:last-child {
  border-bottom: none;
}
</style>
