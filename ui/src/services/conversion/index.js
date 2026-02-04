function getConversionRunDir(conversion) {
  console.log("conversion", conversion);
  // Read output_directory from conversion definition (stored in database)
  const baseDir = conversion?.definition?.output_directory;
  if (!baseDir) {
    console.warn("No output_directory found in conversion definition");
    return null;
  }
  return `${baseDir}/${conversion?.id}`;
}

function getConversionOutputDir(conversion) {
  const runDir = getConversionRunDir(conversion);
  if (!runDir) return null;
  return `${runDir}/${conversion?.dataset?.name}`;
}

function getConversionLogsDir(conversion) {
  const runDir = getConversionRunDir(conversion);
  if (!runDir) return null;
  return `${runDir}/logs`;
}

export { getConversionRunDir, getConversionOutputDir, getConversionLogsDir };
