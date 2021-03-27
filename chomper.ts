import { Context, APIGatewayEvent, APIGatewayProxyResult } from "aws-lambda";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from "@aws-sdk/client-secrets-manager";
import { createInterface } from "readline";
import { once } from "events";
import { Readable } from "stream";
import { decodeStream, encodeStream } from "iconv-lite";
//import moment from "moment";
import { createPool, sql } from "slonik";

export const chomper = async (
  event: APIGatewayEvent,
  context: Context
): Promise<APIGatewayProxyResult> => {
  const tdfId: string = event.queryStringParameters?.tdfId;
  if (!tdfId) {
    return {
      statusCode: 400,
      body: JSON.stringify(
        {
          message: "missing tdfId",
        },
        null,
        2
      ),
    };
  }

  //retrieve the database creds and ready the pool
  const secretClient = new SecretsManagerClient({ region: "us-east-1" });
  const secretCommand = new GetSecretValueCommand({
    SecretId:
      "arn:aws:secretsmanager:us-east-1:474496752274:secret:prod/lfstats-MSO2km",
  });
  let connectionString = "";
  try {
    const { SecretString } = await secretClient.send(secretCommand);
    let secret = JSON.parse(SecretString);
    connectionString = `postgres://${secret.username}:${secret.password}@${secret.host}:${secret.port}/lfstats_tdf`;
  } catch {
    console.log("error");
  }
  const pool = createPool(connectionString);

  //go find the TDF file and get it from S3
  const s3Client = new S3Client({ region: "us-east-1" });
  const s3Command = new GetObjectCommand({
    Bucket: "lfstats-scorecard-archive",
    Key: `${tdfId}.tdf`,
  });

  try {
    const { Body } = await s3Client.send(s3Command);
    if (Body instanceof Readable) {
      const rl = createInterface({
        input: Body.pipe(decodeStream("utf16le")).pipe(encodeStream("utf8")),
        terminal: false,
      });

      rl.on("line", async (line) => {
        console.log(line);
      });
      rl.on("error", async () => {
        console.log("READ ERROR");
      });
      rl.on("close", async () => {
        console.log("READ COMPLETE");
      });
      await once(rl, "close");
    }
  } catch {
    console.log("error");
  }

  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: `${tdfId}`,
      },
      null,
      2
    ),
  };
};
