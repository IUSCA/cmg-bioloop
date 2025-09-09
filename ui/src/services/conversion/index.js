import config from "@/config";

function getConversionRunDir(conversion) {
  console.log("conversion", conversion);
  return `${config.get(`conversions.output_directory`)}/${conversion?.id}`;
}

function getConversionOutputDir(conversion) {
  return `${getConversionRunDir(conversion)}/${conversion?.dataset?.name}`;
}

function getConversionLogsDir(conversion) {
  return `${getConversionRunDir(conversion)}/logs`;
}

export { getConversionRunDir, getConversionOutputDir, getConversionLogsDir };
