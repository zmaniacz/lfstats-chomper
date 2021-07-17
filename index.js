import { S3, SecretsManager } from "aws-sdk";
import { createInterface } from "readline";
import moment from "moment";
import { encodeStream } from "iconv-lite";
import AutoDetectDecoderStream from "autodetect-decoder-stream";
import { createPool, sql } from "slonik";
import { createQueryLoggingInterceptor } from "slonik-interceptor-query-logging";

const interceptors = [createQueryLoggingInterceptor()];

const s3 = new S3({ apiVersion: "2006-03-01" });
const secretsmanager = new SecretsManager({ apiVersion: "2017-10-17" });
const chomperVersion = "1.0.0";

const targetBucket = process.env.TARGET_BUCKET;
let connectionString = "";
//let tdfConnectionString = "";

function getDBCreds() {
  return secretsmanager
    .getSecretValue({
      SecretId:
        "arn:aws:secretsmanager:us-east-1:474496752274:secret:prod/lfstats-MSO2km",
    })
    .promise();
}

export async function handler(event, context) {
  console.log("FIND SECRET");
  try {
    const data = await getDBCreds();
    let secret = JSON.parse(data.SecretString);
    connectionString = `postgres://${secret.username}:${secret.password}@${secret.host}:${secret.port}/lfstats`;
    //tdfConnectionString = `postgres://${secret.username}:${secret.password}@${secret.host}:${secret.port}/lfstats_tdf`;
  } catch (err) {
    console.log("SECRET ERROR", err.stack);
  }

  const params = {
    Bucket: "",
    Key: "",
  };

  let eventId = "";
  console.log("BEGIN CHOMP");
  console.log("Received event:", JSON.stringify(event, null, 2));

  if (event.Records && event.Records[0].eventSource === "aws:s3") {
    console.log("Event is type S3");
    const messageBody = event.Records[0].s3;
    console.log("Message Body: ", JSON.stringify(messageBody, null, 2));
    params.Bucket = messageBody.bucket.name;
    params.Key = decodeURIComponent(messageBody.object.key.replace(/\+/g, " "));

    eventId = params.Key.split("/")[0];
  } else {
    console.log("Event is type API");
  }

  const jobId = context.awsRequestId;

  var entities = new Map();
  var teams = new Map();
  var game = {};
  var actions = [];
  var game_deltas = [];
  var gameId = null;
  const ENTITY_TYPES = {
    1: "Commander",
    2: "Heavy Weapons",
    3: "Scout",
    4: "Ammo Carrier",
    5: "Medic",
  };

  async function chompFile(rl) {
    return new Promise((resolve) => {
      rl.on("line", (line) => {
        let record = line.split("\t");
        if (record[0].includes(';"')) {
          return;
        } else {
          if (record[0] == 0) {
            //;0/info	file-version	program-version	centre
            let location = record[3].split("-");
            game = {
              center: record[3],
              metadata: {
                fileVersion: record[1],
                programVersion: record[2],
                regionCode: location[0],
                siteCode: location[1],
                chomperVersion: chomperVersion,
              },
            };
          } else if (record[0] == 1) {
            //;1/mission	type	desc	start duration penalty
            game = {
              missionType: record[1],
              missionDesc: record[2],
              missionStart: parseInt(record[3]),
              missionStartTime: moment(record[3], "YYYYMMDDHHmmss").format(),
              missionDuration: record[4]
                ? (Math.round(record[4] / 1000) * 1000) / 1000
                : 900,
              missionDurationMillis:
                typeof record[4] != "undefined" ? parseInt(record[4]) : 900000,
              missionLength: null,
              missionLengthMillis: null,
              penaltyValue:
                typeof record[5] != "undefined" ? parseInt(record[5]) : null,
              ...game,
            };
            game.tdfKey = `${game.center}_${game.missionStart}.tdf`;
          } else if (record[0] == 2) {
            //;2/team	index	desc	colour-enum	colour-desc
            //normalize the team colors to either red or green because reasons
            let normalTeam = "";
            if (
              record[4] === "Fire" ||
              record[4] === "Red" ||
              record[4] === "Solid Red"
            ) {
              normalTeam = "red";
            } else {
              normalTeam = "green";
            }

            let team = {
              index: record[1],
              desc: record[2],
              colorEnum: record[3],
              colorDesc: record[4],
              score: 0,
              livesLeft: 0,
              normalTeam: normalTeam,
              lfstatsId: null,
            };
            teams.set(team.index, team);
          } else if (record[0] == 3) {
            //;3/entity-start	time	id	type	desc	team	level	category
            let entity = {
              start: parseInt(record[1]),
              ipl_id: record[2],
              type: record[3],
              desc: record[4],
              team: record[5],
              level: parseInt(record[6]),
              category: record[7],
              position: ENTITY_TYPES[record[7]],
              lfstatsId: null,
              resupplies: 0,
              bases_destroyed: 0,
              rapidFires: [],
              isRapidActive: false,
              shotsFiredDuringRapid: 0,
              shotsHitDuringRapid: 0,
              shotOpponentDuringRapid: 0,
              shotTeamDuringRapid: 0,
              hits: new Map(),
            };
            entities.set(entity.ipl_id, entity);
          } else if (record[0] == 4) {
            //;4/event	time	type	varies
            let action = {
              time: record[1],
              type: record[2],
              player: null,
              action: null,
              target: null,
              team: null,
            };

            let player = null;

            if (record[2] == "0100" || record[2] == "0101") {
              action.action = record[3];
            } else {
              player = entities.get(record[3]);
              action.player = record[3];
              action.team = player.team;
              action.action = record[4];
              action.target =
                typeof record[5] != "undefined" ? record[5] : null;
            }

            actions.push(action);

            //track rapid
            if (record[2] == "0201") {
              if (player.isRapidActive) {
                player.shotsFiredDuringRapid += 1;
              }
            }

            //track and total hits
            if (
              record[2] == "0205" ||
              record[2] == "0206" ||
              record[2] == "0306" ||
              record[2] == "0308"
            ) {
              let target = entities.get(record[5]);

              if (!player.hits.has(target.ipl_id)) {
                player.hits.set(target.ipl_id, {
                  ipl_id: target.ipl_id,
                  hits: 0,
                  missiles: 0,
                });
              }

              if (record[2] == "0205" || record[2] == "0206") {
                player.hits.get(target.ipl_id).hits += 1;
                if (player.isRapidActive) {
                  player.shotsFiredDuringRapid += 1;
                  player.shotsHitDuringRapid += 1;
                  if (player.team === target.team)
                    player.shotTeamDuringRapid += 1;
                  else player.shotOpponentDuringRapid += 1;
                }
              }
              if (record[2] == "0306" || record[2] == "0308")
                player.hits.get(target.ipl_id).missiles += 1;
            }

            //compute game start, end and length
            if (record[2] == "0101") {
              game.missionLength = (Math.round(record[1] / 1000) * 1000) / 1000;
              game.missionLengthMillis = parseInt(record[1]);
              game.endtime = moment(game.missionStart, "YYYYMMDDHHmmss")
                .seconds(game.missionLength)
                .format();
            }

            //track rapid fire starts
            if (record[2] == "0400") {
              player.rapidFires.push({
                rapidStart: parseInt(record[1]),
                rapidEnd: null,
                rapidLength: null,
              });
              player.isRapidActive = true;
            }

            //sum up total resupplies
            if (record[2] == "0500" || record[2] == "0502") {
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
            if (record[2] == "0303" || record[2] == "0204") {
              player.bases_destroyed += 1;
            }
          } else if (record[0] == 5) {
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
          } else if (record[0] == 6) {
            //;6/entity-end	time	id	type	score
            let player = entities.get(record[2]);
            player = {
              end: parseInt(record[1]),
              score: parseInt(record[4]),
              survived:
                (Math.round((record[1] - player.start) / 1000) * 1000) / 1000,
              ...player,
            };
            entities.set(player.ipl_id, player);
          } else if (record[0] == 7) {
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
            teams.get(player.team).livesLeft += player.livesLeft;
            teams.get(player.team).score += player.score;
          }
        }
      });
      rl.on("error", () => {
        console.log("READ ERROR");
      });
      rl.on("close", async () => {
        console.log("READ COMPLETE");
        resolve();
      });
    });
  }

  const pool = createPool(connectionString, { interceptors });
  await pool.connect(async (connection) => {
    await connection.query(sql`
      INSERT INTO game_imports (id, filename, status)
      VALUES (${jobId}, ${params.Key}, ${"starting chomp..."})
    `);

    try {
      const rl = createInterface({
        input: s3
          .getObject(params)
          .createReadStream()
          .pipe(new AutoDetectDecoderStream())
          .pipe(encodeStream("utf8")),
        terminal: false,
      });

      await chompFile(rl);

      const storageParams = {
        CopySource: params.Bucket + "/" + params.Key,
        Bucket: targetBucket,
        Key: game.tdfKey,
      };

      await s3
        .copyObject(storageParams)
        .promise()
        .then((data) => console.log("MOVED TDF TO ARCHIVE", data))
        .catch((err) => console.log(err, err.stack));

      await s3
        .deleteObject(params)
        .promise()
        .then((data) => console.log("REMOVED TDF", data))
        .catch((err) => console.log(err, err.stack));

      //IMPORT PROCESS
      await connection.query(sql`
        UPDATE game_imports
        SET status = ${"importing game..."}
        WHERE id = ${jobId}
      `);

      //Let's see if the game already exists before we start doing too much
      let gameExist = await connection.maybeOne(
        sql`SELECT games.id 
            FROM games 
            INNER JOIN centers ON games.center_id=centers.id 
            WHERE game_datetime=${game.missionStartTime} AND centers.ipl_id=${game.center}`
      );

      if (gameExist != null) {
        console.log("CHOMP ABORTED: game exists");
        await connection.query(sql`
          UPDATE game_imports
          SET status = ${"game exists, import aborted"}, job_end=now()
          WHERE id = ${jobId}
        `);
        return;
      }

      let event = await connection.one(sql`
        SELECT *
        FROM events 
        WHERE id=${eventId}
      `);

      await connection.query(sql`
        UPDATE game_imports
        SET center_id=${event.center_id}
        WHERE id = ${jobId}
      `);

      let playerRecords = await connection.transaction(async (client) => {
        //find or create lfstats player IDs
        //baller screaver optimization
        return await client.query(sql`
          INSERT INTO players (player_name,ipl_id) 
          VALUES (
            ${sql.join(
              [...entities]
                .filter((p) => p[1].type === "player")
                .sort()
                .map((p) => sql.join([p[1].desc, p[1].ipl_id], sql`, `)),
              sql`), (`
            )}
          )
          ON CONFLICT (ipl_id) DO UPDATE SET player_name=excluded.player_name
          RETURNING *
        `);
      });

      //update our entities with their lfstats IDs for future reference
      for (let player of playerRecords.rows) {
        entities.get(player.ipl_id).lfstatsId = player.id;
      }

      //upsert aliases
      await connection.transaction(async (client) => {
        return await client.query(sql`
          INSERT INTO players_names (player_id,player_name,is_active) 
          VALUES 
          (
            ${sql.join(
              [...entities]
                .filter(
                  (p) => p[1].type === "player" && p[1].ipl_id.startsWith("#")
                )
                .sort()
                .map((p) =>
                  sql.join([p[1].lfstatsId, p[1].desc, true], sql`, `)
                ),
              sql`), (`
            )} 
          )
          ON CONFLICT (player_id,player_name) DO UPDATE SET is_active=true
        `);
      });

      await connection.transaction(async (client) => {
        //start working on game details pre-insert
        //need to normalize team colors and determine elims before inserting the game
        let redTeam;
        let greenTeam;
        // eslint-disable-next-line no-unused-vars
        for (const [key, value] of teams) {
          if (value.normalTeam == "red") redTeam = value;
          if (value.normalTeam == "green") greenTeam = value;
        }

        //Assign elim bonuses
        let greenBonus = 0,
          redBonus = 0;
        let redElim = 0,
          greenElim = 0;
        if (redTeam.livesLeft == 0) {
          greenBonus = 10000;
          redElim = 1;
        }
        if (greenTeam.livesLeft == 0) {
          redBonus = 10000;
          greenElim = 1;
        }

        //assign a winner
        let winner = "";
        //if both teams were elimed or neither were, we go to score
        //otherwise, winner determined by elim regardless of score
        if (redElim == greenElim) {
          if (redTeam.score + redBonus > greenTeam.score + greenBonus)
            winner = "red";
          else winner = "green";
        } else if (redElim) winner = "green";
        else if (greenElim) winner = "red";
        game.name = `Game @ ${moment(game.missionStartTime).format("HH:mm")}`;

        let gameRecord = await client.query(sql`
          INSERT INTO games 
            (game_name,game_description,game_datetime,game_length,duration,red_score,green_score,red_adj,green_adj,winner,red_eliminated,green_eliminated,type,center_id,event_id,tdf_key)
          VALUES
            (${game.name},'',${game.missionStartTime},${game.missionLength},${game.missionDuration},${redTeam.score},${greenTeam.score},${redBonus},${greenBonus},${winner},${redElim},${greenElim},${event.type},${event.center_id},${event.id},${game.tdfKey})
          RETURNING *
        `);
        let newGame = gameRecord.rows[0];
        gameId = newGame.id;

        await client.query(sql`
          UPDATE game_imports
          SET status = ${"importing actions..."}
          WHERE id = ${jobId}
        `);

        //insert the actions
        let chunkSize = 100;
        for (let i = 0, len = actions.length; i < len; i += chunkSize) {
          let chunk = actions.slice(i, i + chunkSize);
          await client.query(sql`
            INSERT INTO game_actions
              (action_time, action_type, action_text, player, target, team_index, game_id) 
            VALUES (
              ${sql.join(
                chunk.map((action) =>
                  sql.join(
                    [
                      action.time,
                      action.type,
                      action.action,
                      action.player,
                      action.target,
                      action.team,
                      newGame.id,
                    ],
                    sql`, `
                  )
                ),
                sql`), (`
              )}
            )
          `);
        }

        //insert the score deltas
        for (let i = 0, len = game_deltas.length; i < len; i += chunkSize) {
          let chunk = game_deltas.slice(i, i + chunkSize);
          await client.query(sql`
            INSERT INTO game_deltas
              (score_time, old, delta, new, ipl_id, player_id, team_index, game_id) 
            VALUES (
              ${sql.join(
                chunk.map((delta) =>
                  sql.join(
                    [
                      delta.time,
                      delta.old,
                      delta.delta,
                      delta.new,
                      delta.player,
                      null,
                      delta.team,
                      newGame.id,
                    ],
                    sql`, `
                  )
                ),
                sql`), (`
              )}
            )
          `);
        }

        //store non-player objects
        //should be referees and targets/generators
        await client.query(sql`
          INSERT INTO game_objects (ipl_id,name,type,team,level,category,game_id) 
          VALUES 
          (
            ${sql.join(
              [...entities]
                .filter((r) => r[1].type != "player")
                .map((r) =>
                  sql.join(
                    [
                      r[1].ipl_id,
                      r[1].desc,
                      r[1].type,
                      r[1].team,
                      r[1].level,
                      r[1].category,
                      newGame.id,
                    ],
                    sql`, `
                  )
                ),
              sql`), (`
            )} 
          )
        `);

        //insert the teams
        let teamRecords = await client.query(sql`
          INSERT INTO game_teams (index,name,color_enum,color_desc,color_normal,game_id) 
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
                    t[1].normalTeam,
                    newGame.id,
                  ],
                  sql`, `
                )
              ),
              sql`), (`
            )} 
          )
          RETURNING *
        `);

        for (let team of teamRecords.rows) {
          teams.get(`${team.index}`).lfstatsId = team.id;
          await client.query(sql`
            UPDATE game_actions
            SET team_id = ${team.id}
            WHERE team_index = ${team.index}
              AND
                  game_id = ${newGame.id}
          `);
          await client.query(sql`
            UPDATE game_deltas
            SET team_id = ${team.id}
            WHERE team_index = ${team.index}
              AND
                  game_id = ${newGame.id}
        `);
        }

        await client.query(sql`
          UPDATE game_imports
          SET status = ${"importing scorecards..."}
          WHERE id = ${jobId}
        `);

        //insert the scorecards
        // eslint-disable-next-line no-unused-vars
        for (const [key, player] of entities) {
          if (player.type == "player") {
            let team = teams.get(player.team);

            let team_elim = 0;
            let elim_other_team = 0;
            if (
              (redElim && team.normalTeam == "red") ||
              (greenElim && team.normalTeam == "green")
            )
              team_elim = 1;
            if (
              (redElim && team.normalTeam == "green") ||
              (greenElim && team.normalTeam == "red")
            )
              elim_other_team = 1;

            let scorecardRecord = await client.query(sql`
                  INSERT INTO scorecards
                    (
                      player_name,
                      game_datetime,
                      team,
                      position,
                      survived,
                      uptime,
                      resupply_downtime,
                      other_downtime,
                      shots_hit,
                      shots_fired,
                      times_zapped,
                      times_missiled,
                      missile_hits,
                      nukes_activated,
                      nukes_detonated,
                      nukes_canceled,
                      medic_hits,
                      own_medic_hits,
                      medic_nukes,
                      scout_rapid,
                      life_boost,
                      ammo_boost,
                      lives_left,
                      score,
                      max_score,
                      shots_left,
                      penalty_count,
                      shot_3hit,
                      elim_other_team,
                      team_elim,
                      own_nuke_cancels,
                      shot_opponent,
                      shot_team,
                      missiled_opponent,
                      missiled_team,
                      resupplies,
                      rank,
                      bases_destroyed,
                      accuracy,
                      hit_diff,
                      mvp_points,
                      mvp_details,
                      sp_earned,
                      sp_spent,
                      game_id,
                      type,
                      player_id,
                      center_id,
                      event_id,
                      team_id,
                      rapid_fire_history,
                      shots_fired_during_rapid,
                      shots_hit_during_rapid,
                      shot_opponent_during_rapid,
                      shot_team_during_rapid
                    )
                  VALUES
                    (
                      ${player.desc},
                      ${game.missionStartTime},
                      ${team.normalTeam},
                      ${player.position},
                      ${player.survived},
                      ${null},
                      ${null},
                      ${null},
                      ${player.shotsHit},
                      ${player.shotsFired},
                      ${player.timesZapped},
                      ${player.timesMissiled},
                      ${player.missileHits},
                      ${player.nukesActivated},
                      ${player.nukesDetonated},
                      ${player.nukeCancels},
                      ${player.medicHits},
                      ${player.ownMedicHits},
                      ${player.medicNukes},
                      ${player.scoutRapid},
                      ${player.lifeBoost},
                      ${player.ammoBoost},
                      ${player.livesLeft},
                      ${player.score},
                      0,
                      ${player.shotsLeft},
                      ${player.penalties},
                      ${player.shot3Hit},
                      ${elim_other_team},
                      ${team_elim},
                      ${player.ownNukeCancels},
                      ${player.shotOpponent},
                      ${player.shotTeam},
                      ${player.missiledOpponent},
                      ${player.missiledTeam},
                      ${player.resupplies},
                      0,
                      ${player.bases_destroyed},
                      ${player.accuracy},
                      ${player.hit_diff},
                      0,
                      ${null},
                      ${player.sp_earned},
                      ${player.sp_spent},
                      ${newGame.id},
                      ${event.type},
                      ${player.lfstatsId},
                      ${event.center_id},
                      ${event.id},
                      ${team.lfstatsId},
                      ${JSON.stringify(player.rapidFires)},
                      ${player.shotsFiredDuringRapid},
                      ${player.shotsHitDuringRapid},
                      ${player.shotOpponentDuringRapid},
                      ${player.shotTeamDuringRapid}
                    )
                    RETURNING *
                `);
            player.scorecard_id = scorecardRecord.rows[0].id;
          }
        }

        //Let's iterate through the entities and make some udpates in the database
        // eslint-disable-next-line no-unused-vars
        for (let [key, player] of entities) {
          if (player.type == "player") {
            //1-Tie an internal lfstats id to players and targets in each action
            await client.query(sql`
                  UPDATE game_actions
                  SET player_id = ${player.lfstatsId}
                  WHERE player = ${player.ipl_id}
                    AND
                        game_id = ${newGame.id}
                `);
            await client.query(sql`
                  UPDATE game_actions
                  SET target_id = ${player.lfstatsId}
                  WHERE target = ${player.ipl_id}
                    AND
                        game_id = ${newGame.id}
                `);
            //2-Tie an internal lfstats id to each score delta
            await client.query(sql`
                  UPDATE game_deltas
                  SET player_id = ${player.lfstatsId}
                  WHERE ipl_id = ${player.ipl_id}
                    AND
                        game_id = ${newGame.id}
                `);
            //3-insert the hit and missile stats for each player
            // eslint-disable-next-line no-unused-vars
            for (let [key, target] of player.hits) {
              if (entities.has(target.ipl_id)) {
                target.target_lfstatsId = entities.get(target.ipl_id).lfstatsId;
              }
              await client.query(sql`
                  INSERT INTO hits
                    (player_id, target_id, hits, missiles, scorecard_id)
                  VALUES
                    (${player.lfstatsId}, ${target.target_lfstatsId}, ${target.hits}, ${target.missiles}, ${player.scorecard_id})
                `);
            }
            //4-fix penalties
            if (player.penalties > 0) {
              if (game.penaltyValue === null) {
                // no defined penalty value, so all defaulted to 1k and need to back them out
                let penalties = await client.many(sql`
                  SELECT *
                  FROM game_deltas
                  WHERE game_id = ${newGame.id}
                    AND
                      player_id = ${player.lfstatsId}
                    AND
                      delta = -1000
                  ORDER BY score_time ASC
                `);
                for (const penalty of penalties) {
                  //log the penalty - just going to use the common defaults
                  await client.query(sql`
                    INSERT INTO penalties
                      (scorecard_id)
                    VALUES
                      (${player.scorecard_id})
                  `);
                  //fix the player's score
                  await client.query(sql`
                    UPDATE scorecards
                    SET score=score+1000
                    WHERE id=${player.scorecard_id}
                  `);
                  //Now the tricky bit, have to rebuild the score deltas from the point the penalty occurred
                  //update the delta event to remove the -1000
                  await client.query(sql`
                    UPDATE game_deltas 
                    SET delta=0,new=new+1000 
                    WHERE id=${penalty.id}
                  `);
                  //Now update a lot of rows, so scary
                  await client.query(sql`
                    UPDATE game_deltas 
                    SET old=old+1000,new=new+1000 
                    WHERE game_id = ${newGame.id}
                      AND
                        player_id = ${player.lfstatsId}
                      AND
                        score_time>${penalty.score_time}
                  `);
                }
              } else {
                //we're in the new model and everything is better
                for (let i = 0; i < player.penalties; i++) {
                  await client.query(sql`
                    INSERT INTO penalties
                      (value, scorecard_id)
                    VALUES
                      (${game.penaltyValue}, ${player.scorecard_id})
                  `);
                }
              }
            }
          }
        }

        await client.query(sql`
          UPDATE game_imports
          SET status = ${"calculating mvp..."}
          WHERE id = ${jobId}
        `);

        //calc mvp - lets fuckin go bro, the good shit aw yiss
        let scorecards = await client.many(sql`
              SELECT scorecards.*, players.ipl_id
              FROM scorecards
              LEFT JOIN players ON scorecards.player_id = players.id
              WHERE game_id = ${newGame.id}
            `);

        for (const scorecard of scorecards) {
          //instantiate the fuckin mvp object bro
          let mvp = 0;
          let mvpDetails = {
            positionBonus: {
              name: "Position Score Bonus",
              value: 0,
            },
            missiledOpponent: {
              name: "Missiled Opponent",
              value: 0,
            },
            acc: {
              name: "Accuracy",
              value: 0,
            },
            nukesDetonated: {
              name: "Nukes Detonated",
              value: 0,
            },
            nukesCanceled: {
              name: "Nukes Canceled",
              value: 0,
            },
            medicHits: {
              name: "Medic Hits",
              value: 0,
            },
            ownMedicHits: {
              name: "Own Medic Hits",
              value: 0,
            },
            /*rapidFire: {
              name: "Activate Rapid Fire",
              value: 0
            },*/
            shoot3Hit: {
              name: "Shoot 3-Hit",
              value: 0,
            },
            ammoBoost: {
              name: "Ammo Boost",
              value: 0,
            },
            lifeBoost: {
              name: "Life Boost",
              value: 0,
            },
            medicSurviveBonus: {
              name: "Medic Survival Bonus",
              value: 0,
            },
            medicScoreBonus: {
              name: "Medic Score Bonus",
              value: 0,
            },
            elimBonus: {
              name: "Elimination Bonus",
              value: 0,
            },
            timesMissiled: {
              name: "Times Missiled",
              value: 0,
            },
            missiledTeam: {
              name: "Missiled Team",
              value: 0,
            },
            ownNukesCanceled: {
              name: "Your Nukes Canceled",
              value: 0,
            },
            teamNukesCanceled: {
              name: "Team Nukes Canceled",
              value: 0,
            },
            elimPenalty: {
              name: "Elimination Penalty",
              value: 0,
            },
            penalties: {
              name: "Penalties",
              value: 0,
            },
          };

          //POSITION BASED SCORE BONUS OMFG GIT GUD
          switch (scorecard.position) {
            case "Ammo Carrier":
              mvpDetails.positionBonus.value += Math.max(
                Math.floor((scorecard.score - 3000) / 10) * 0.01,
                0
              );
              break;
            case "Commander":
              mvpDetails.positionBonus.value += Math.max(
                Math.floor((scorecard.score - 10000) / 10) * 0.01,
                0
              );
              break;
            case "Heavy Weapons":
              mvpDetails.positionBonus.value += Math.max(
                Math.floor((scorecard.score - 7000) / 10) * 0.01,
                0
              );
              break;
            case "Medic":
              mvpDetails.positionBonus.value += Math.max(
                Math.floor((scorecard.score - 2000) / 10) * 0.02,
                0
              );
              break;
            case "Scout":
              mvpDetails.positionBonus.value += Math.max(
                Math.floor((scorecard.score - 6000) / 10) * 0.01,
                0
              );
              break;
          }

          //medic bonus score point - removed on 2020-02-22
          /*if ("Medic" == scorecard.position && scorecard.score >= 3000) {
            mvpDetails.medicScoreBonus.value += 1;
          }*/

          //accuracy bonus
          mvpDetails.acc.value += Math.round(scorecard.accuracy * 100) / 10;

          //don't get missiled dummy
          mvpDetails.timesMissiled.value += scorecard.times_missiled * -1;

          //missile other people instead
          switch (scorecard.position) {
            case "Commander":
              mvpDetails.missiledOpponent.value += scorecard.missiled_opponent;
              break;
            case "Heavy Weapons":
              mvpDetails.missiledOpponent.value +=
                scorecard.missiled_opponent * 2;
              break;
          }

          //get dat 5-chain
          mvpDetails.nukesDetonated.value += scorecard.nukes_detonated;

          //maybe hide better
          if (scorecard.nukes_activated - scorecard.nukes_detonated > 0) {
            let team = "red" == scorecard.team ? "green" : "red";

            let nukes = await client.one(sql`
                  SELECT SUM(nukes_canceled) as all_nukes_canceled
                  FROM scorecards
                  WHERE game_id = ${newGame.id} AND team = ${team}
                `);

            if (nukes.all_nukes_canceled > 0) {
              mvpDetails.ownNukesCanceled.value +=
                nukes.all_nukes_canceled * -1;
            }
          }

          //make commanders cry
          mvpDetails.nukesCanceled.value += scorecard.nukes_canceled * 3;

          //medic tears are scrumptious
          mvpDetails.medicHits.value += scorecard.medic_hits;

          //dont be a venom
          mvpDetails.ownMedicHits.value += scorecard.own_medic_hits * -1;

          //push the little button
          //mvpDetails.rapidFire.value += scorecard.scout_rapid * 0.5;
          mvpDetails.lifeBoost.value += scorecard.life_boost * 3;
          mvpDetails.ammoBoost.value += scorecard.ammo_boost * 3;

          //survival bonuses/penalties
          if (scorecard.lives_left > 0 && "Medic" == scorecard.position) {
            mvpDetails.medicSurviveBonus.value += 2;
          }

          if (scorecard.lives_left <= 0 && "Medic" != scorecard.position) {
            mvpDetails.elimPenalty.value += -1;
          }

          //apply penalties based on value of the penalty
          let playerPenalties = await client.any(sql`
                SELECT *
                FROM penalties
                WHERE scorecard_id = ${scorecard.id}
              `);
          for (let penalty of playerPenalties) {
            if ("Penalty Removed" != penalty.type) {
              mvpDetails.penalties.value += penalty.mvp_value;
            }
          }

          //raping 3hits.  the math looks weird, but it works and gets the desired result
          mvpDetails.shoot3Hit.value +=
            Math.floor((scorecard.shot_3hit / 5) * 100) / 100;

          //One time DK, one fucking time.
          mvpDetails.teamNukesCanceled.value += scorecard.own_nuke_cancels * -3;

          //more venom points
          mvpDetails.missiledTeam.value += scorecard.missiled_team * -3;

          //WINNER
          //at least 1 MVP for an elim, increased by 1/60 for each second of time remaining over 60
          if (scorecard.elim_other_team > 0)
            mvpDetails.elimBonus.value += Number.parseFloat(
              Math.max(
                1.0,
                (newGame.duration - newGame.game_length) / 60
              ).toFixed(2)
            );

          //sum it up and insert
          for (const prop in mvpDetails) {
            mvp += mvpDetails[prop].value;
          }
          mvp = Math.max(0, mvp);

          // unrelated to mvp - calculate downtime caused by either resupply or other deactivation, and uptime
          const player = entities.get(scorecard.ipl_id);

          let uptime = 0;
          let resupplyDowntime = 0;
          let otherDowntime = 0;

          // the action codes for various deactivation events
          const resuppliedActionCodes = ["0500", "0502"];
          const deactivatedActionCodes = ["0206", "0306", "0308", "0600"];

          const deacs = actions
            .filter(
              (action) =>
                action.time < player.end &&
                // enemy nuke detonated
                ((action.team !== player.team && action.type === "0405") ||
                  (action.target === player.ipl_id &&
                    // resupplied (x2), shot, missiled or penalised
                    [
                      ...resuppliedActionCodes,
                      ...deactivatedActionCodes,
                    ].includes(action.type)))
            )
            .sort((a, b) => a.time - b.time);

          deacs.forEach((deac, i) => {
            // this may not exist, e.g. when i === 0
            const prevDeac = deacs[i - 1];
            // this may not exist, e.g. when i === deacs.length - 1
            const nextDeac = deacs[i + 1];

            // calculate how long the player was down as a result of this deactivation.
            // if the player was deactivated again before they came up (e.g. reset, nuke), then this deac duration is less than 8s
            const deacDuration = Math.min(
              ((nextDeac && nextDeac.time) || player.end) - deac.time,
              8000
            );

            // calculate how much time passed between the previous deactivation and this one.
            const deacTimeDiff =
              deac.time - ((prevDeac && prevDeac.time) || player.start);

            // now calculate any uptime the player had between the deacs
            // if the player was deactivated again before they came up (e.g. reset, nuke), then this will be 0
            const uptimeDuration = Math.max(deacTimeDiff - 8000, 0);

            if ([...resuppliedActionCodes].includes(deac.type)) {
              // this was a resupply deactivation
              resupplyDowntime += deacDuration;
            } else {
              // this was a non-resupply deactivation
              otherDowntime += deacDuration;
            }

            // add the uptime to the total, if any
            uptime += uptimeDuration;
          });

          // add uptime from the final deac up to the player survived/eliminated time
          let lastDeac = deacs[deacs.length - 1];
          uptime +=
            player.end - (lastDeac && lastDeac.time ? lastDeac.time : 0);

          await client.query(sql`
                UPDATE scorecards
                SET mvp_points=${mvp}, mvp_details=${JSON.stringify(
            mvpDetails
          )}, uptime=${uptime}, resupply_downtime=${resupplyDowntime}, other_downtime=${otherDowntime}
                WHERE id = ${scorecard.id}
              `);
        }
      });

      console.log("CHOMP COMPLETE");

      await connection.query(sql`
        UPDATE game_imports
        SET status = ${"success"},job_end=now(),game_id=${gameId}
        WHERE id = ${jobId}
      `);
    } catch (err) {
      console.log("CHOMP ERROR", err.stack);
      await pool.connect(async (connection) => {
        await connection.query(sql`
        UPDATE game_imports
        SET status = ${"failed"},job_end=now()
        WHERE id = ${jobId}
      `);
      });
    }
  });
}
