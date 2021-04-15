import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import { APIGatewayEvent, APIGatewayProxyResult } from "aws-lambda";
import { once } from "events";
import { decodeStream, encodeStream } from "iconv-lite";
import { DateTime } from "luxon";
import { createInterface } from "readline";
import { createQueryLoggingInterceptor } from "slonik-interceptor-query-logging";
import { Readable } from "stream";
import {
  DeacType,
  Entity,
  EntityState,
  EntityType,
  Game,
  GameAction,
  GameMetaData,
  Team,
} from "types";
import {
  defaultInitialState,
  entityTypes,
  positionDefaults,
} from "./constants";
import * as _ from "lodash";

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
  let currentState = new Map<string, EntityState>();
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
            initialState: { ...defaultInitialState },
            finalState: null,
            lfstatsId: null,
          } as Entity;

          //set up initial numbners based on the entity type
          entity.initialState.shots = entity.initialShots;
          entity.initialState.lives = entity.initialLives;
          entity.initialState.missilesLeft = entity.initialMissiles;
          entity.initialState.currentHP = entity.maxHP;

          entities.set(entity.ipl_id, entity);
          currentState.set(entity.ipl_id, { ...entity.initialState });
        } else if (record[0] === "4") {
          //;4/event	time	type	varies
          let action = {
            time: parseInt(record[1]),
            type: record[2],
            player: null,
            action: null,
            target: null,
            state: _.cloneDeep(currentState),
          } as GameAction;
          // EventMissionStart 0100
          // EventMissionEnd 0101
          // EventShotEmpty 0200 - unused?
          // EventShotMiss 0201
          // EventShotGenMiss 0202
          // EventShotGenDamage 0203
          // EventShotGenDestroy 0204
          // EventShotOppDamage 0205
          // EventShotOppDown 0206
          // EventShotOwnDamage 0207 - unused?
          // EventShotOwnDown 0208 - unused?
          // EventMslStart 0300 - no state change
          // EventMslGenMiss 0301
          // EventMslGenDamage 0302 - unused probably?
          // EventMslGenDestroy 0303
          // EventMslMiss 0304
          // EventMslOppDamage 0305
          // EventMslOppDown 0306
          // EventMslOwnDamage 0307
          // EventMslOwnDown 0308
          // EventRapidAct 0400
          // EventRapidDeac 0401
          // EventNukeAct 0404
          // EventNukeDeton 0405
          // EventResupplyShots 0500
          // EventResupplyLives 0502
          // EventResupplyTeamShots 0510
          // EventResupplyTeamLives 0512
          // EventPenalty 0600
          // EventAchieve 0900

          if (action.type === "0100" || action.type === "0101") {
            action.action = record[3];
            //compute game start, end and length
            if (action.type === "0101") {
              game.missionLength =
                (Math.round(parseInt(record[1]) / 1000) * 1000) / 1000;
              game.missionLengthMillis = parseInt(record[1]);
            }
          } else {
            let playerState = action.state.get(record[3]) as EntityState;
            let player = entities.get(record[3]) as Entity;
            let targetState = action.state.get(record[5]) as EntityState;
            let target = entities.get(record[5]) as Entity;
            action.player = record[3];
            action.action = record[4];
            action.target = record?.[5] ?? null;

            //Check if player isActive
            //if false, then the last time we saw this player, they were deaced
            //create a synthetic reactivation action, set the time as lastDeacTime + 8000
            //we cant actually update state until al the actions ar eparsed and loaded
            //then go back through adn apply each action to the initial state

            // EventShotMiss 0201
            // EventShotGenMiss 0202
            // EventShotGenDamage 0203
            // EventShotGenDestroy 0204
            if (
              action.type === "0201" ||
              action.type === "0202" ||
              action.type === "0203" ||
              action.type === "0204"
            ) {
              playerState.shots -= 1;
              playerState.shotsFired += 1;
              if (playerState.isRapidActive) {
                playerState.shotsFiredDuringRapid += 1;
              }
              if (action.type === "0203") playerState.shotBase += 1;
              if (action.type === "0204") {
                playerState.destroyBase += 1;
                playerState.spEarned += 5;
                playerState.score += 1001;
              }
            }

            // EventShotOppDamage 0205
            // Only occurs against a 3-hit
            if (action.type === "0205") {
              //update the player
              playerState.shots -= 1;
              playerState.shotsFired += 1;
              playerState.shotsHit += 1;
              playerState.shotOpponent += 1;
              playerState.shot3Hit += 1;
              if (playerState.isRapidActive) {
                playerState.shotsFiredDuringRapid += 1;
                playerState.shotsHitDuringRapid += 1;
                playerState.shotOpponentDuringRapid += 1;
                playerState.shot3HitDuringRapid += 1;
              } else {
                playerState.spEarned += 1;
              }
              playerState.score += 100;

              targetState.selfHit += 1;
              targetState.currentHP -= player.shotPower;
              targetState.score -= 20;
            }

            // EventShotOppDown 0206
            if (action.type === "0206") {
              //update the player
              playerState.shots -= 1;
              playerState.shotsFired += 1;
              playerState.shotsHit += 1;
              playerState.shotOpponent += 1;
              playerState.deacOpponent += 1;
              if (playerState.isRapidActive) {
                playerState.shotsFiredDuringRapid += 1;
                playerState.shotsHitDuringRapid += 1;
                playerState.shotOpponentDuringRapid += 1;
                playerState.deacOpponentDuringRapid += 1;
              } else {
                playerState.spEarned += 1;
              }
              playerState.score += 100;

              if (
                target.position === EntityType.Commander ||
                target.position === EntityType.Heavy
              ) {
                playerState.shot3Hit += 1;
                playerState.deac3Hit += 1;
                if (playerState.isRapidActive) {
                  playerState.shot3HitDuringRapid += 1;
                  playerState.deac3HitDuringRapid += 1;
                }
              }

              if (target.position === EntityType.Medic) {
                playerState.medicHits += 1;
                if (playerState.isRapidActive) {
                  playerState.medicHitsDuringRapid += 1;
                }
              }

              targetState.selfHit += 1;
              targetState.currentHP = 0;
              targetState.lastDeacTime = action.time;
              targetState.lastDeacType = DeacType.Opponent;
              targetState.isActive = false;
              targetState.score -= 20;
            }

            // EventMslGenMiss 0301
            if ((action.type = "0301")) {
              playerState.missilesLeft -= 1;
            }

            // EventMslGenDestroy 0303
            if ((action.type = "0303")) {
              playerState.missilesLeft -= 1;
              playerState.destroyBase += 1;
              playerState.missileBase += 1;
              playerState.score += 1001;
              playerState.spEarned += 5;
            }

            //track rapid fire starts
            if (record[2] === "0400") {
              playerState.isRapidActive = true;
              playerState.spSpent += 20;
            }

            //track and total hits
            if (
              record[2] === "0205" ||
              record[2] === "0206" ||
              record[2] === "0306" ||
              record[2] === "0308"
            ) {
              let target = entities.get(record[5]) as Entity;

              if (record[2] === "0205" || record[2] === "0206") {
                if (playerState.isRapidActive) {
                  playerState.shotsFiredDuringRapid += 1;
                  playerState.shotsHitDuringRapid += 1;
                  if (player.team === target.team)
                    playerState.shotTeamDuringRapid += 1;
                  else playerState.shotOpponentDuringRapid += 1;
                }
              }
            }

            //sum up total resupplies
            if (record[2] === "0500" || record[2] === "0502") {
              let targetState = action.state.get(record[5]) as EntityState;
              if (record[2] === "0500") playerState.resupplyShots += 1;
              if (record[2] === "0502") playerState.resupplyLives += 1;
              //if rapid fire is active on the target, now it's over
              targetState.isRapidActive = false;
              /*if (targetState.rapidFires.length > 0) {
                let rapidStatus =
                  targetState.rapidFires[targetState.rapidFires.length - 1];
                rapidStatus.rapidEnd = parseInt(record[1]);
                rapidStatus.rapidLength =
                  rapidStatus.rapidEnd - rapidStatus.rapidStart;
              }*/
            }

            //sum up total bases destroyed
            if (record[2] === "0303" || record[2] === "0204") {
              playerState.destroyBase += 1;
            }
          }

          actions.set(action.time, action);
        } else if (record[0] === "5") {
          let player = entities.get(record[2]) as Entity;
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
          let player = entities.get(record[2]) as Entity;
          player = {
            endTime: parseInt(record[1]),
            endCode: parseInt(record[3]),
            ...player,
          };
          entities.set(player.ipl_id, player);
        } else if (record[0] === "7") {
          //;7/sm5-stats	id	shotsHit	shotsFired	timesZapped	timesMissiled	missileHits	nukesDetonated	nukesActivated	nukeCancels	medicHits	ownMedicHits	medicNukes	scoutRapid	lifeBoost	ammoBoost	livesLeft	shotsLeft	penalties	shot3Hit	ownNukeCancels	shotOpponent	shotTeam	missiledOpponent	missiledTeam
          let playerState = currentState.get(record[1]) as EntityState;

          //clean up rapid
          if (playerState.isRapidActive) {
            playerState.isRapidActive = false;
            /*let rapidStatus = player.rapidFires[player.rapidFires.length - 1];
            rapidStatus.rapidEnd = player.end;
            rapidStatus.rapidLength =
              rapidStatus.rapidEnd - rapidStatus.rapidStart;*/
          }

          /*player = {
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

          entities.set(record[1], player);*/
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
    console.log("CHOMP: READ ERROR");
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

  /*try {
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
        for (let action of actions) {
          action.playerLfstatsId =
            entities.get(action.player)?.lfstatsId ?? null;
          action.targetLfstatsId =
            entities.get(action.target)?.lfstatsId ?? null;
        }

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
  }*/

  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: `${tdfId} chomped successfully`,
        entities: "",
      },
      replacer,
      2
    ),
  };
};

function replacer(key: any, value: any) {
  if (value instanceof Map) {
    return {
      dataType: "Map",
      value: [...value],
    };
  } else {
    return value;
  }
}
