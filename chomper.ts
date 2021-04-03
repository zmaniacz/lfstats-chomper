import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import { APIGatewayEvent, APIGatewayProxyResult } from "aws-lambda";
import { entityTypes, positionDefaults } from "./constants";
import { once } from "events";
import { decodeStream, encodeStream } from "iconv-lite";
import { DateTime } from "luxon";
import { createInterface } from "readline";
import { createPool, sql } from "slonik";
import { createQueryLoggingInterceptor } from "slonik-interceptor-query-logging";
import { Readable } from "stream";
import { Game, GameMetaData, Entity, Team, GameAction } from "types";

export const chomper = async (
  event: APIGatewayEvent
): Promise<APIGatewayProxyResult> => {
  const chomperVersion = "2.0.0";
  const tdfId = event.queryStringParameters?.tdfId;
  const interceptors = [createQueryLoggingInterceptor()];
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
    let { SecretString } = await secretClient.send(secretCommand);
    if (SecretString) {
      let secret = JSON.parse(SecretString);
      connectionString = `postgres://${secret.username}:${secret.password}@${secret.host}:${secret.port}/lfstats_tdf`;
    } else throw "secret error";
  } catch (error) {
    return {
      statusCode: 502,
      body: JSON.stringify(
        {
          message: "secret error",
        },
        null,
        2
      ),
    };
  }

  //go find the TDF file and get it from S3
  //then read line by line and load to the db
  const s3Client = new S3Client({ region: "us-east-1" });
  const s3Command = new GetObjectCommand({
    Bucket: "lfstats-scorecard-archive",
    Key: `${tdfId}.tdf`,
  });

  //object to store our results fo aprsing
  let game: Game;
  let gameMetaData: GameMetaData;
  let entities = new Map<string, Entity>();
  let teams = new Map<number, Team>();
  let actions = new Map<number, GameAction>();
  let game_deltas = [];
  //let gameId = null;

  try {
    const { Body } = await s3Client.send(s3Command);
    if (Body instanceof Readable) {
      const rl = createInterface({
        input: Body.pipe(decodeStream("utf16le")).pipe(encodeStream("utf8")),
        terminal: false,
      });

      rl.on("line", async (line) => {
        //it is a tab delimited file after all
        let record = line.split("\t");
        if (record[0] === "0") {
          //;0/info	file-version	program-version	centre
          let location = record[3].split("-");
          gameMetaData = {
            fileVersion: record[1],
            programVersion: record[2],
            regionCode: location[0],
            siteCode: location[1],
            chomperVersion: chomperVersion,
            tdfKey: tdfId,
          } as GameMetaData;
        } else if (record[0] === "1") {
          //;1/mission	type	desc	start duration penalty
          game = {
            missionType: record[1],
            missionDesc: record[2],
            missionStart: parseInt(record[3]),
            missionStartTime: DateTime.fromFormat(record[3], "yyyyMMddHHmmss", {
              zone: "utc",
            }).toSQL({ includeOffset: false }),
            missionDuration: record[4]
              ? (Math.round(parseInt(record[4]) / 1000) * 1000) / 1000
              : 900,
            missionDurationMillis: parseInt(record?.[4]) ?? 900000,
            missionLength: null,
            missionLengthMillis: null,
            penaltyValue: parseInt(record?.[5]) ?? null,
          } as Game;
        } else if (record[0] === "2") {
          //;2/team	index	desc	colour-enum	colour-desc
          let team = {
            index: parseInt(record[1]),
            desc: record[2],
            colorEnum: parseInt(record[3]),
            colorDesc: record[4],
            lfstatsId: null,
          } as Team;
          teams.set(team.index, team);
        } else if (record[0] === "3") {
          //;3/entity-start	time	id	type	desc	team	level	category
          let position = entityTypes[parseInt(record[7])];
          let entity = {
            startTime: parseInt(record[1]),
            ipl_id: record[2],
            type: record[3],
            desc: record[4],
            team: parseInt(record[5]),
            level: parseInt(record[6]),
            category: parseInt(record[7]),
            position: position,
            battlesuit: record?.[8] ?? null,
            endCode: null,
            ...positionDefaults[position],
            initialState: null,
            finalState: null,
            lfstatsId: null,
          } as Entity;
          entities.set(entity.ipl_id, entity);
        } else if (record[0] === "4") {
          //;4/event	time	type	varies
          let action = {
            time: parseInt(record[1]),
            type: record[2],
            player: null,
            action: null,
            target: null,
          } as GameAction;

          if (record[2] === "0100" || record[2] === "0101") {
            action.action = record[3];
            //compute game start, end and length
            if (record[2] === "0101") {
              game.missionLength =
                (Math.round(parseInt(record[1]) / 1000) * 1000) / 1000;
              game.missionLengthMillis = parseInt(record[1]);
            }
          } else {
            let player = entities.get(record[3]);
            action.player = record[3];
            action.action = record[4];
            action.target = record?.[5] ?? null;

            //Check if player isActive
            //if false, then the last time we saw this player, they were deaced
            //create a synthetic reactivation action, set the time as lastDeacTime + 8000
            //we cant actually update state until al the actions ar eparsed and loaded
            //then go back through adn apply each action to the initial state

            //track rapid fire starts
            if (record[2] === "0400") {
              /*player.rapidFires.push({
                rapidStart: parseInt(record[1]),
                rapidEnd: null,
                rapidLength: null,
              });*/
              player.isRapidActive = true;
            }

            //track rapid fire misses
            //we don't have to total shots separately since the entity end line gives us that total
            if (record[2] === "0201") {
              if (player.isRapidActive) {
                player.shotsFiredDuringRapid += 1;
              }
            }

            //track and total hits
            if (
              record[2] === "0205" ||
              record[2] === "0206" ||
              record[2] === "0306" ||
              record[2] === "0308"
            ) {
              let target = entities.get(record[5]);

              if (record[2] === "0205" || record[2] === "0206") {
                if (player.isRapidActive) {
                  player.shotsFiredDuringRapid += 1;
                  player.shotsHitDuringRapid += 1;
                  if (player.team === target.team)
                    player.shotTeamDuringRapid += 1;
                  else player.shotOpponentDuringRapid += 1;
                }
              }
            }

            //sum up total resupplies
            if (record[2] === "0500" || record[2] === "0502") {
              let target = entities.get(record[5]);
              player.resupplies += 1;
              //if rapid fire is active on the target, now it's over
              target.isRapidActive = false;
              if (target.rapidFires.length > 0) {
                let rapidStatus =
                  target.rapidFires[target.rapidFires.length - 1];
                rapidStatus.rapidEnd = parseInt(record[1]);
                rapidStatus.rapidLength =
                  rapidStatus.rapidEnd - rapidStatus.rapidStart;
              }
            }

            //sum up total bases destroyed
            if (record[2] === "0303" || record[2] === "0204") {
              player.bases_destroyed += 1;
            }
          }

          actions.set(action.time, action);
        } else if (record[0] === "5") {
          let player = entities.get(record[2]);
          //;5/score	time	entity	old	delta	new
          game_deltas.push({
            time: record[1],
            player: record[2],
            team: player.team,
            old: record[3],
            delta: record[4],
            new: record[5],
          });
        } else if (record[0] === "6") {
          //;6/entity-end	time	id	type	score
          let player = entities.get(record[2]);
          player = {
            end: parseInt(record[1]),
            score: parseInt(record[4]),
            endCode: parseInt(record[3]),
            eliminated: parseInt(record[3]) === 4 ? true : false,
            survived:
              (Math.round((parseInt(record[1]) - player.start) / 1000) * 1000) /
              1000,
            survivedMillis: parseInt(record[1]) - player.start,
            ...player,
          };
          entities.set(player.ipl_id, player);
        } else if (record[0] === "7") {
          //;7/sm5-stats	id	shotsHit	shotsFired	timesZapped	timesMissiled	missileHits	nukesDetonated	nukesActivated	nukeCancels	medicHits	ownMedicHits	medicNukes	scoutRapid	lifeBoost	ammoBoost	livesLeft	shotsLeft	penalties	shot3Hit	ownNukeCancels	shotOpponent	shotTeam	missiledOpponent	missiledTeam
          let player = entities.get(record[1]);

          //clean up rapid
          if (player.isRapidActive) {
            player.isRapidActive = false;
            let rapidStatus = player.rapidFires[player.rapidFires.length - 1];
            rapidStatus.rapidEnd = player.end;
            rapidStatus.rapidLength =
              rapidStatus.rapidEnd - rapidStatus.rapidStart;
          }

          player = {
            accuracy: parseInt(record[2]) / Math.max(parseInt(record[3]), 1),
            hit_diff: parseInt(record[21]) / Math.max(parseInt(record[4]), 1),
            sp_earned:
              parseInt(record[21]) +
              parseInt(record[23]) * 2 +
              player.bases_destroyed * 5,
            sp_spent:
              parseInt(record[8]) * 20 +
              parseInt(record[14]) * 10 +
              parseInt(record[15]) * 15,
            shotsHit: parseInt(record[2]),
            shotsFired: parseInt(record[3]),
            timesZapped: parseInt(record[4]),
            timesMissiled: parseInt(record[5]),
            missileHits: parseInt(record[6]),
            nukesDetonated: parseInt(record[7]),
            nukesActivated: parseInt(record[8]),
            nukeCancels: parseInt(record[9]),
            medicHits: parseInt(record[10]),
            ownMedicHits: parseInt(record[11]),
            medicNukes: parseInt(record[12]),
            scoutRapid: parseInt(record[13]),
            lifeBoost: parseInt(record[14]),
            ammoBoost: parseInt(record[15]),
            livesLeft: parseInt(record[16]),
            shotsLeft: parseInt(record[17]),
            penalties: parseInt(record[18]),
            shot3Hit: parseInt(record[19]),
            ownNukeCancels: parseInt(record[20]),
            shotOpponent: parseInt(record[21]),
            shotTeam: parseInt(record[22]),
            missiledOpponent: parseInt(record[23]),
            missiledTeam: parseInt(record[24]),
            ...player,
          };

          entities.set(record[1], player);
        }
      });
      rl.on("error", async () => {
        console.log("READ ERROR");
      });
      rl.on("close", async () => {
        console.log("READ COMPLETE");
      });
      await once(rl, "close");
    }
  } catch (error) {
    console.log("CHOMP2: READ ERROR");
    const { requestId, cfId, extendedRequestId } = error.$metadata;
    console.log({ requestId, cfId, extendedRequestId });
    return {
      statusCode: 502,
      body: JSON.stringify(
        {
          message: "read error",
        },
        null,
        2
      ),
    };
  }

  try {
    const pool = createPool(connectionString, { interceptors });
    await pool.connect(async (connection) => {
      await connection.transaction(async (client) => {
        //Insert and retrieve a record for the center
        let centerRecord = await client.maybeOne(sql`
          SELECT *
          FROM center
          WHERE region_code = ${gameMetaData.regionCode} AND site_code = ${gameMetaData.siteCode}
        `);

        if (!centerRecord) {
          let centerName = `Unknown ${gameMetaData.regionCode}-${gameMetaData.siteCode}`;
          centerRecord = await client.one(sql`
            INSERT INTO center 
              (name,short_name,region_code,site_code)
            VALUES
              (${centerName},'unk',${gameMetaData.regionCode},${gameMetaData.siteCode})
            RETURNING *
          `);
        }

        //insert players
        let playerRecords = await client.any(sql`
          INSERT INTO player (ipl_id, current_alias)
          VALUES (
            ${sql.join(
              [...entities]
                .filter((p) => p[1].type === "player")
                .sort()
                .map((p) => sql.join([p[1].ipl_id, p[1].desc], sql`, `)),
              sql`), (`
            )}
          )
          ON CONFLICT (ipl_id) DO UPDATE SET current_alias = EXCLUDED.current_alias
          RETURNING *
        `);

        //update our entities with their lfstats IDs for future reference
        for (let player of playerRecords) {
          entities.get(player.ipl_id as string).lfstatsId = player.id as number;
        }

        //insert current aliases
        await connection.many(sql`
          INSERT INTO player_alias (alias,last_used,player_id)
          VALUES (
            ${sql.join(
              [...entities]
                .filter((p) => p[1].type === "player")
                .sort()
                .map((p) =>
                  sql.join(
                    [p[1].desc, game.missionStartTime, p[1].lfstatsId],
                    sql`, `
                  )
                ),
              sql`), (`
            )}
          )
          ON CONFLICT (alias,player_id) DO UPDATE SET last_used=EXCLUDED.last_used
          RETURNING *
        `);

        //Insert and retrieve a record for the game
        let gameExist = await client.exists(
          sql`SELECT id 
            FROM game 
            WHERE mission_start=${game.missionStartTime} AND center_id=${centerRecord.id}`
        );

        if (gameExist) {
          console.log("CHOMP2 ABORTED: game exists");
          return;
        }

        let gameRecord = await client.one(sql`
            INSERT INTO game 
              (
                mission_type,
                mission_desc,
                mission_start,
                mission_max_length,
                penalty,
                mission_length,
                center_id,
                file_version,
                program_version,
                chomper_version
              )
            VALUES
              (
                ${game.missionType},
                ${game.missionDesc},
                ${game.missionStartTime},
                ${game.missionDurationMillis},
                ${game.penaltyValue},
                ${game.missionLengthMillis},
                ${centerRecord.id},
                ${gameMetaData.fileVersion},
                ${gameMetaData.programVersion},
                ${gameMetaData.chomperVersion}
              )
            RETURNING *
          `);

        //on to the teams
        let gameTeamRecords = await client.many(sql`
          INSERT INTO game_team
            (
              team_index,
              team_desc,
              color_enum,
              color_desc,
              game_id
            )
          VALUES
            (
              ${sql.join(
                [...teams].map((t) =>
                  sql.join(
                    [
                      t[1].index,
                      t[1].desc,
                      t[1].colorEnum,
                      t[1].colorDesc,
                      gameRecord.id,
                    ],
                    sql`, `
                  )
                ),
                sql`), (`
              )} 
            )
          RETURNING *
        `);

        for (let entity of entities)
          for (let gameTeamRecord of gameTeamRecords)
            if (gameTeamRecord.team_index === entity[1].team)
              entity[1].gameTeamId = gameTeamRecord.id;

        //now the entities
        let gameEntityRecords = await client.many(sql`
          INSERT INTO game_entity
            (
              ipl_id,
              entity_type,
              entity_desc,
              entity_level,
              category,
              battlesuit,
              game_team_id,
              end_code,
              eliminated,
              end_time,
              score,
              position,
              start_time,
            )
          VALUES 
            (
              ${sql.join(
                [...entities].map((e) =>
                  sql.join(
                    [
                      e[1].ipl_id,
                      e[1].type,
                      e[1].desc,
                      e[1].level,
                      e[1].category,
                      e[1].battlesuit,
                      e[1].gameTeamId,
                      e[1].endCode,
                      e[1].eliminated,
                      e[1].end,
                      e[1].score,
                      e[1].position,
                      e[1].start,
                    ],
                    sql`, `
                  )
                ),
                sql`), (`
              )} 
            )
          RETURNING *
        `);

        for (let gameEntityRecord of gameEntityRecords) {
          entities.get(gameEntityRecord.ipl_id).gameEntityLfstatsId =
            gameEntityRecord.id;
        }

        //explicitly load lfstats ids into the actions object
        /*for (let action of actions) {
          action.playerLfstatsId =
            entities.get(action.player)?.lfstatsId ?? null;
          action.targetLfstatsId =
            entities.get(action.target)?.lfstatsId ?? null;
        }*/

        //insert the actions
        let chunkSize = 100;
        for (let i = 0, len = actions.length; i < len; i += chunkSize) {
          let chunk = actions.slice(i, i + chunkSize);
          await client.query(sql`
            INSERT INTO game_action
              (action_time, action_type, action_text, actor_id, target_id, game_id) 
            VALUES (
              ${sql.join(
                chunk.map((action) =>
                  sql.join(
                    [
                      action.time,
                      action.type,
                      action.action,
                      entities.get(action.player)?.gameEntityLfstatsId ?? null,
                      entities.get(action.target)?.gameEntityLfstatsId ?? null,
                      gameRecord.id,
                    ],
                    sql`, `
                  )
                ),
                sql`), (`
              )}
            )
          `);
        }
      });
    });
  } catch (error) {
    console.log("CHOMP2: DB ERROR");
    console.log(error.stack);
    return {
      statusCode: 502,
      body: JSON.stringify(
        {
          message: "DB error",
        },
        null,
        2
      ),
    };
  }

  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: `${tdfId} chomped successfully`,
        game,
      },
      null,
      2
    ),
  };
};
