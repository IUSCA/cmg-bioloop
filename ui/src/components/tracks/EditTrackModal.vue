<template>
  <va-modal
    v-model="visible"
    title="Edit Track"
    size="large"
    @ok="handleSubmit"
    @cancel="handleCancel"
    :ok-disabled="!canSubmit"
    :loading="loading"
  >
    <div class="space-y-6">
      <!-- Track Name -->
      <va-input
        v-model="form.name"
        label="Track Name"
        placeholder="Enter track name"
        :error="errors.name"
        required
      />

      <!-- File Type -->
      <va-select
        v-model="form.file_type"
        :options="fileTypeOptions"
        label="File Type"
        placeholder="Select file type"
        :error="errors.file_type"
        required
      />

      <!-- Genome Type -->
      <va-select
        v-model="form.genome_type"
        :options="genomeTypeOptions"
        label="Genome Type"
        placeholder="Select genome type"
        :error="errors.genome_type"
        required
      />

      <!-- Genome Value -->
      <va-select
        v-model="form.genome_value"
        :options="genomeOptions"
        label="Genome Version"
        placeholder="Select genome version"
        :error="errors.genome_value"
        required
        :disabled="!form.genome_type"
      />

      <!-- Dataset Information (Read-only) -->
      <div class="bg-gray-50 p-4 rounded-lg">
        <h4 class="font-medium mb-2">Dataset Information</h4>
        <div class="space-y-2 text-sm">
          <div class="flex justify-between">
            <span class="text-gray-600">Dataset:</span>
            <span>{{ track?.dataset_file?.dataset?.name || 'Unknown' }}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-gray-600">File:</span>
            <span>{{ track?.dataset_file?.name || 'Unknown' }}</span>
          </div>
          <div class="flex justify-between">
            <span class="text-gray-600">Size:</span>
            <span>{{ formatFileSize(track?.dataset_file?.size) }}</span>
          </div>
        </div>
      </div>
    </div>
  </va-modal>
</template>

<script setup>
import constants from '@/constants';
import { useTracksStore } from '@/stores/tracks';
import { computed, ref, watch } from 'vue';
import { toast } from 'vue-toastification';

const props = defineProps({
  modelValue: { type: Boolean, required: true },
  track: { type: Object, default: null },
});

const emit = defineEmits(['update:modelValue', 'updated']);

const tracksStore = useTracksStore();

// Reactive state
const visible = ref(false);
const loading = ref(false);
const form = ref({
  name: '',
  file_type: '',
  genome_type: '',
  genome_value: '',
});
const errors = ref({});

// Computed
const fileTypeOptions = computed(() => {
  return [
    { text: 'BAM', value: 'bam' },
    { text: 'BigWig', value: 'bigwig' },
    { text: 'VCF', value: 'vcf' },
    { text: 'BED', value: 'bed' },
    { text: 'GTF', value: 'gtf' },
    { text: 'FASTQ', value: 'fastq' },
    { text: 'FASTA', value: 'fasta' },
  ];
});

const genomeTypeOptions = computed(() => {
  return Object.keys(constants.GENOME_TYPES).map(type => ({
    text: type.charAt(0).toUpperCase() + type.slice(1),
    value: type,
  }));
});

const genomeOptions = computed(() => {
  if (!form.value.genome_type) return [];
  
  const genomes = constants.GENOME_TYPES[form.value.genome_type]?.genomes || [];
  return genomes.map(genome => ({
    text: genome,
    value: genome,
  }));
});

const canSubmit = computed(() => {
  return form.value.name.trim() && 
         form.value.file_type && 
         form.value.genome_type &&
         form.value.genome_value;
});

// Watchers
watch(() => props.modelValue, (newValue) => {
  visible.value = newValue;
  if (newValue && props.track) {
    initializeForm();
  }
});

watch(visible, (newValue) => {
  emit('update:modelValue', newValue);
});

watch(() => form.value.genome_type, () => {
  // Reset genome value when genome type changes
  form.value.genome_value = '';
});

// Methods
const initializeForm = () => {
  if (!props.track) return;
  
  form.value = {
    name: props.track.name || '',
    file_type: props.track.file_type || '',
    genome_type: props.track.genomeType || '',
    genome_value: props.track.genomeValue || '',
  };
  
  errors.value = {};
};

const formatFileSize = (bytes) => {
  if (!bytes) return 'Unknown';
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
};

const validateForm = () => {
  errors.value = {};
  
  if (!form.value.name.trim()) {
    errors.value.name = 'Track name is required';
  }
  
  if (!form.value.file_type) {
    errors.value.file_type = 'File type is required';
  }
  
  if (!form.value.genome_type) {
    errors.value.genome_type = 'Genome type is required';
  }
  
  if (!form.value.genome_value) {
    errors.value.genome_value = 'Genome version is required';
  }
  
  return Object.keys(errors.value).length === 0;
};

const handleSubmit = async () => {
  if (!validateForm()) {
    return;
  }
  
  loading.value = true;
  
  try {
    const updateData = {
      name: form.value.name.trim(),
      file_type: form.value.file_type,
      genome_type: form.value.genome_type,
      genome_value: form.value.genome_value,
    };
    
    await tracksStore.updateTrack(props.track.id, updateData);
    toast.success('Track updated successfully');
    emit('updated', props.track);
    visible.value = false;
  } catch (error) {
    console.error('Failed to update track:', error);
    toast.error('Failed to update track');
  } finally {
    loading.value = false;
  }
};

const handleCancel = () => {
  visible.value = false;
};
</script>
