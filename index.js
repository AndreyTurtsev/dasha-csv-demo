const fs = require("fs");
const moment = require("moment");
const guid = require("node-uuid");
const dasha = require("@dasha.ai/sdk");
const csvParser = require("csv-parser");
const csvWriter = require("csv-writer");

const callsData = new Map();

const csvReportHeaders = [
  { id: "phone", title: "Phone" },
  { id: "status", title: "Status" },
  { id: "serviceStatus", title: "Service Status" },
  { id: "record", title: "RecordUrl" },
  { id: "jobStatus", title: "Job Status" },
  { id: "errors", title: "Errors" },
];

function createCsvReport(filePath, headers) {
  return csvWriter.createObjectCsvWriter({
    alwaysQuote: true,
    path: filePath,
    header: headers,
  });
}

function writeResult(writer, timestamp, input, output, customData) {
  writer.writeRecords([{ timestamp, ...input, ...output, ...customData }]);
}

async function loadCallsFromCsv(filePath, csvParams) {
  const data = [];
  for await (const csv_row of fs
    .createReadStream(filePath)
    .pipe(csvParser(csvParams))) {
    data.push(csv_row);
  }

  return data;
}

async function main() {
  const app = await dasha.deploy("./app", { groupName: "Default" });

  const csvInput = process.argv[2];
  let csvOutput = process.argv[3];
  if (!csvOutput) {
    csvOutput = `${moment().format("YYYY-MM-DD_HH-mm-ss")}.csv`;
    console.log(`path for report file was not specified`);
    console.log(`report will be saved as ${csvOutput}`);
  }

  let writer = createCsvReport(csvOutput, csvReportHeaders);

  app.queue.on("ready", async (key, conv) => {
    const data = callsData.get(key);
    if (!data) {
      console.log(`Can't find data was job with key ${key}`);
      console.log(`Rejecting ${key}`);
      await conv.ignore();

      writeResult(writer, now, null, null, {
        key,
        jobStatus: "Rejected",
      });
      return;
    }

    conv.input = data;
    conv.audio.noiseVolume = 0.1;
    conv.sip.config = "default";
    conv.audio.tts = "default";

    try {
      const result = await conv.execute();
      console.log(result.output);
      const now = moment();
      writeResult(writer, now, conv.input, result.output, {
        key,
        jobStatus: "Completed",
        record: result.recordingUrl,
      });
    } catch (error) {
      const now = moment();
      console.error(
        `Job ${key} execution was failed. Error: ${error.name}: ${error.message}`
      );
      writeResult(writer, now, conv.input, null, {
        key,
        jobStatus: "Failed",
      });
    } finally {
      callsData.delete(key);
      if (callsData.size == 0) {
        onEnded();
      }
    }
  });

  app.queue.on("error", (error) => {
    console.error(
      `Error ${error.name}:${error.message}. Reason ${error.reason}`
    );
  });

  app.queue.on("rejected", (key, error) => {
    const now = moment();
    console.warn(
      `Job ${key} was rejected. Error ${error.name}:${error.message}. Reason ${error.reason}`
    );
    writeResult(writer, now, callsData.get(key), null, {
      key,
      jobStatus: "Rejected",
    });
    callsData.delete(key);
    if (callsData.size == 0) {
      onEnded();
    }
  });

  app.queue.on("timeout", (key) => {
    const now = moment();
    console.log(`Job ${key} was timed out`);
    writeResult(writer, now, callsData.get(key), null, {
      key,
      jobStatus: "Timeout",
    });
    callsData.delete(key);
    if (callsData.size == 0) {
      onEnded();
    }
  });

  const data = await loadCallsFromCsv(csvInput);
  for (const dataRecord of data) {
    const uniqueId = guid.v4();
    callsData.set(uniqueId, dataRecord);
    app.queue.push(uniqueId);
  }
  console.log(`${callsData.size} call(s) was enqueued`);
  await app.start({ concurrency: 10 });
}

function timeout(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function onEnded() {
  console.log(`Calls ended. Waiting 10 seconds to close application...`);
  await timeout(10000);
  process.exit(0);
}
main();
