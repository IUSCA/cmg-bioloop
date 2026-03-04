#!/usr/bin/env node
/* eslint-disable no-console */

const { main } = require('../src/scripts/insert_mock_tracks');

main()
  .then(() => {
    console.log('✅ Mock tracks and sessions data insertion completed successfully!');
    console.log('\n📊 Created test data:');
    console.log('   • 5 research projects');
    console.log('   • 6 datasets (Data Products)');
    console.log('   • Multiple dataset files per dataset');
    console.log('   • Tracks for supported file types (bam, vcf, bigwig, fastq)');
    console.log('   • 8 genome browser sessions with various configurations');
    console.log('   • Session-track relationships (6 sessions with tracks, 2 pristine)');
    console.log('   • Project assignments for datasets (tracks inherit access)');
    console.log('   • e2eUser assigned to all projects and sessions');
    console.log('\n🎯 You can now test the tracks and sessions features with this comprehensive test data!');
    console.log('\n🔍 Test scenarios:');
    console.log('   • Sessions with multiple tracks (different genome types)');
    console.log('   • Pristine sessions (no tracks assigned)');
    console.log('   • Public vs private sessions');
    console.log('   • Track filtering and search functionality');
    console.log('   • Session management and editing');
    console.log('\n📈 Data Summary:');
    console.log('   • ~59 dataset files created');
    console.log('   • ~37 tracks created (from trackable file types)');
    console.log('   • 8 sessions created (6 with tracks, 2 pristine)');
    console.log('   • 14 session-track relationships established');
    process.exit(0);
  })
  .catch((error) => {
    console.error('❌ Mock tracks and sessions data insertion failed:', error);
    process.exit(1);
  });
