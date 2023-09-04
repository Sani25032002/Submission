const crypto = require('crypto');
const localDataMap = require('./local');
const cloudDataMap = require('./cloud');
const batchSize = Math.ceil(localDataMap.size / 10); // Batch size of 100K
const hashAlgorithm = 'sha256';

async function generateHash(data) {
  const hash = crypto.createHash(hashAlgorithm);
  hash.update(data);
  return hash.digest('hex');
}

async function verifyBatch(batchIds) {
  let errors = 0; // Track errors within the batch

  const batchPromises = batchIds.map(async (id) => {
    const localItem = localDataMap.get(id);
    const cloudItem = cloudDataMap.get(id);

    if (!localItem || !cloudItem) {
      console.log(`Item with ID ${id} missing in data`);
      return;
    }

    const localHash = await generateHash(canonicalJson(localItem));
    const cloudHash = await generateHash(canonicalJson(cloudItem));

    if (localHash !== cloudHash) {
      errors++;
      console.log(`Data mismatch for item with ID ${id}`);
    }
  });

  await Promise.all(batchPromises);

  return errors; // Return the number of errors in the batch
}

function canonicalJson(obj) {
  return JSON.stringify(obj, Object.keys(obj).sort());
}

async function main() {
  let count = 0;

  const localDataKeys = Array.from(localDataMap.keys());
  const totalBatches = Math.ceil(localDataKeys.length / batchSize);

  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    const batchStart = batchIndex * batchSize;
    const batchEnd = batchStart + batchSize;
    const localBatchIds = localDataKeys.slice(batchStart, batchEnd);

    const batchErrors = await verifyBatch(localBatchIds);

    // Update the count with errors from the current batch
    count += batchErrors;
  }

  if (count > 0) {
    console.log(`Data verification failed with ${count} errors`);
  } else {
    console.log('Data verification successful without mismatches');
  }

  console.log('Exiting the function...');
}

main().catch((err) => console.error('Error:', err));
