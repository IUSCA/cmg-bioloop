// PM2 Ecosystem Configuration for Genomic Data Testing Scripts
// https://pm2.keymetrics.io/docs/usage/application-declaration/

module.exports = {
  apps: [
    // This file is a placeholder for managing test dataset registration scripts.
    // These scripts are typically run manually on-demand rather than as long-running processes.
    // 
    // To run a registration script:
    //   poetry shell
    //   python -m workers.scripts.genomic_data_testing.register_sequencing_runs.runs.register_iseq_di
    //   python -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigwig
    //
    // Uncomment the entries below if you want to run them as managed PM2 processes
    
    // {
    //   name: "register_iseq_di",
    //   script: "python",
    //   args: "-u -m workers.scripts.genomic_data_testing.register_sequencing_runs.runs.register_iseq_di",
    //   watch: false,
    //   interpreter: "",
    //   autorestart: false,
    //   log_date_format: "YYYY-MM-DD HH:mm Z",
    //   error_file: "../../../../logs/workers/genomic_testing/register_iseq_di.err",
    //   out_file: "../../../../logs/workers/genomic_testing/register_iseq_di.log",
    // },
    // {
    //   name: "register_bigwig",
    //   script: "python",
    //   args: "-u -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigwig",
    //   watch: false,
    //   interpreter: "",
    //   autorestart: false,
    //   log_date_format: "YYYY-MM-DD HH:mm Z",
    //   error_file: "../../../../logs/workers/genomic_testing/register_bigwig.err",
    //   out_file: "../../../../logs/workers/genomic_testing/register_bigwig.log",
    // },
    // {
    //   name: "register_bigbed",
    //   script: "python",
    //   args: "-u -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_bigbed",
    //   watch: false,
    //   interpreter: "",
    //   autorestart: false,
    //   log_date_format: "YYYY-MM-DD HH:mm Z",
    //   error_file: "../../../../logs/workers/genomic_testing/register_bigbed.err",
    //   out_file: "../../../../logs/workers/genomic_testing/register_bigbed.log",
    // },
    // {
    //   name: "register_methylation",
    //   script: "python",
    //   args: "-u -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_methylation",
    //   watch: false,
    //   interpreter: "",
    //   autorestart: false,
    //   log_date_format: "YYYY-MM-DD HH:mm Z",
    //   error_file: "../../../../logs/workers/genomic_testing/register_methylation.err",
    //   out_file: "../../../../logs/workers/genomic_testing/register_methylation.log",
    // },
    // {
    //   name: "register_gsm429321_h3k27ac",
    //   script: "python",
    //   args: "-u -m workers.scripts.genomic_data_testing.register_data_products_suitable_for_genome_browser.products.register_gsm429321_h3k27ac",
    //   watch: false,
    //   interpreter: "",
    //   autorestart: false,
    //   log_date_format: "YYYY-MM-DD HH:mm Z",
    //   error_file: "../../../../logs/workers/genomic_testing/register_gsm429321_h3k27ac.err",
    //   out_file: "../../../../logs/workers/genomic_testing/register_gsm429321_h3k27ac.log",
    // },
    // {
    //   name: "register_h3k27ac",
    //   script: "python",
    //   args: "-u -m workers.scripts.genomic_data_testing.register_genome_browser_suitable_data_products.products.register_h3k27ac",
    //   watch: false,
    //   interpreter: "",
    //   autorestart: false,
    //   log_date_format: "YYYY-MM-DD HH:mm Z",
    //   error_file: "../../../../logs/workers/genomic_testing/register_h3k27ac.err",
    //   out_file: "../../../../logs/workers/genomic_testing/register_h3k27ac.log",
    // }
  ]
};

