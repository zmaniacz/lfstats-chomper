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
import { last } from "lodash";

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

  //object to store our resultsof parsing
  let game: Game;
  let gameMetaData: GameMetaData;
  let entities = new Map<string, Entity>();
  let teams = new Map<number, Team>();
  let actions: GameAction[] = [];
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

          //set up initial numbers based on the entity type
          entity.initialState.ipl_id = entity.ipl_id;
          entity.initialState.shots = entity.initialShots;
          entity.initialState.lives = entity.initialLives;
          entity.initialState.missilesLeft = entity.initialMissiles;
          entity.initialState.currentHP = entity.maxHP;

          entities.set(entity.ipl_id, entity);
          currentState.set(entity.ipl_id, { ...entity.initialState });
        } else if (record[0] === "4") {
          //;4/event	time	type	varies
          let action: GameAction = {
            time: parseInt(record[1]),
            type: record[2],
            player: record?.[3] ?? null,
            action: record?.[4] ?? null,
            target: record?.[5] ?? null,
            state: null,
          };

          if (action.type === "0100" || action.type === "0101") {
            action.action = record[3];
            action.player = null;
            action.target = null;
            //compute game start, end and length
            if (action.type === "0101") {
              game.missionLength =
                (Math.round(action.time / 1000) * 1000) / 1000;
              game.missionLengthMillis = action.time;
            }
          }

          actions.push(action);
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
          /*let playerState = currentState.get(record[1]) as EntityState;
          let player = entities.get(record[1]) as Entity;*/
          //clean up rapid
          /*if (playerState.isRapidActive) {
            playerState.isRapidActive = false;
            let rapidStatus = player.rapidFires[player.rapidFires.length - 1];
            rapidStatus.rapidEnd = player.end;
            rapidStatus.rapidLength =
              rapidStatus.rapidEnd - rapidStatus.rapidStart;
          }*/
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
          };*/
          /*player.finalState = _.cloneDeep(playerState);
          entities.set(player.ipl_id, player);*/
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

  //make sure our actions array is in time slice order
  actions.sort((a, b) => {
    return a.time - b.time;
  });

  let newActions: GameAction[] = [];
  //initialize entity state for creating reac events
  //this jsut holds an IplID and a lastdeactime - which will be null if the player is online
  let tempStates = new Map<string, number | null>();
  for (let entity of entities.values()) {
    if (entity.type === "player") {
      tempStates.set(entity.ipl_id, null);
    }
  }

  //for any deac event, set or update the target's last deac time
  //a nuke resets it for all opposing team players
  for (let action of actions) {
    if (
      action.type === "0206" ||
      action.type === "0306" ||
      action.type === "0500" ||
      action.type === "0502" ||
      action.type === "0600"
    ) {
      tempStates.set(action.target as string, action.time);
    }
    if (action.type === "0405") {
      //nuke
      let player = entities.get(action.player as string) as Entity;
      for (let [IplId, target] of entities) {
        if (target.type === "player" && player.team !== target.team) {
          tempStates.set(IplId as string, action.time);
        }
      }
    }
    for (let [IplId, lastDeacTime] of tempStates) {
      if (lastDeacTime && lastDeacTime + 8000 <= action.time) {
        newActions.push({
          time: lastDeacTime + 8000,
          type: "LFS001",
          action: " reactivated",
          player: IplId,
          target: null,
          state: null,
        });
        tempStates.set(IplId, null);
      }
    }
  }

  actions = [...actions, ...newActions];
  actions.sort((a, b) => {
    return a.time - b.time;
  });

  //now the fun of state application begins
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
  // EventMslGenDamage 0302 - unused?
  // EventMslGenDestroy 0303
  // EventMslMiss 0304
  // EventMslOppDamage 0305 - unused?
  // EventMslOppDown 0306
  // EventMslOwnDamage 0307 - unused?
  // EventMslOwnDown 0308 - unused?
  // EventRapidAct 0400
  // EventRapidDeac 0401 - unused?
  // EventNukeAct 0404
  // EventNukeDeton 0405
  // EventResupplyShots 0500
  // EventResupplyLives 0502
  // EventResupplyTeamShots 0510
  // EventResupplyTeamLives 0512
  // EventPenalty 0600
  // EventAchieve 0900

  for (let action of actions) {
    let playerState = currentState.get(action.player as string) as EntityState;
    let player = entities.get(action.player as string) as Entity;
    let targetState = currentState.get(action.target as string) as EntityState;
    let target = entities.get(action.target as string) as Entity;

    // EventShotMiss 0201
    // EventShotGenMiss 0202
    if (action.type === "0201" || action.type === "0202") {
      playerState.shots -= 1;
      playerState.shotsFired += 1;
      if (playerState.isRapidActive) {
        playerState.shotsFiredDuringRapid += 1;
      }
    }

    // EventShotGenDamage 0203
    // EventShotGenDestroy 0204
    if (action.type === "0203" || action.type === "0204") {
      playerState.shots -= 1;
      playerState.shotsFired += 1;
      playerState.shotsHit += 1;
      if (playerState.isRapidActive) {
        playerState.shotsFiredDuringRapid += 1;
      }
      if (action.type === "0203") playerState.shotBase += 1;
      if (action.type === "0204") {
        playerState.shotBase += 1;
        playerState.destroyBase += 1;
        // no check for rapid fire because Than said so
        playerState.spEarned += 5;
        playerState.score += 1001;
      }
    }

    // EventShotOppDamage 0205
    // Only occurs against a 3-hit
    // can be opponent or teammate
    if (action.type === "0205") {
      playerState.shots -= 1;
      playerState.shotsFired += 1;
      playerState.shotsHit += 1;

      if (player.team === target.team) {
        playerState.shotTeam += 1;
        playerState.score -= 100;
      } else {
        playerState.shotOpponent += 1;
        playerState.shot3Hit += 1;
        playerState.score += 100;
      }

      if (playerState.isRapidActive) {
        playerState.shotsFiredDuringRapid += 1;
        playerState.shotsHitDuringRapid += 1;

        if (player.team === target.team) {
          playerState.shotTeamDuringRapid += 1;
        } else {
          playerState.shotOpponentDuringRapid += 1;
          playerState.shot3HitDuringRapid += 1;
        }
      } else {
        if (player.team !== target.team) playerState.spEarned += 1;
      }

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

      if (player.team === target.team) {
        playerState.shotTeam += 1;
        playerState.deacTeam += 1;
        playerState.score -= 100;
      } else {
        playerState.shotOpponent += 1;
        playerState.deacOpponent += 1;
        playerState.score += 100;
      }

      if (playerState.isRapidActive) {
        playerState.shotsFiredDuringRapid += 1;
        playerState.shotsHitDuringRapid += 1;

        if (player.team === target.team) {
          playerState.shotTeamDuringRapid += 1;
          playerState.deacTeamDuringRapid += 1;
        } else {
          playerState.shotOpponentDuringRapid += 1;
          playerState.deacOpponentDuringRapid += 1;
        }
      } else {
        if (player.team !== target.team) playerState.spEarned += 1;
      }

      if (
        player.team !== target.team &&
        (target.position === EntityType.Commander ||
          target.position === EntityType.Heavy)
      ) {
        playerState.shot3Hit += 1;
        playerState.deac3Hit += 1;
        if (playerState.isRapidActive) {
          playerState.shot3HitDuringRapid += 1;
          playerState.deac3HitDuringRapid += 1;
        }
      }

      if (target.position === EntityType.Medic) {
        if (player.team === target.team) {
          playerState.ownMedicHits += 1;
        } else {
          playerState.medicHits += 1;
          if (playerState.isRapidActive) {
            playerState.medicHitsDuringRapid += 1;
          }
        }
      }

      targetState.selfHit += 1;
      //reset HP so we can track while down
      targetState.currentHP = target.maxHP;
      targetState.lives = Math.max(targetState.lives - 1, 0);
      targetState.selfDeac += 1;
      targetState.lastDeacTime = action.time;
      if (player.team === target.team) {
        targetState.lastDeacType = DeacType.Team;
      } else {
        targetState.lastDeacType = DeacType.Opponent;
      }
      targetState.isActive = false;
      targetState.score -= 20;
      if (targetState.isRapidActive) {
        targetState.selfDeacDuringRapid += 1;
      }
      if (targetState.isNuking) {
        targetState.isNuking = false;
        if (player.team === target.team) {
          targetState.ownNukeCanceledByTeam += 1;
          playerState.cancelTeamNuke += 1;
        } else {
          targetState.ownNukeCanceledByOpponent += 1;
          playerState.cancelOpponentNuke += 1;
        }
      }
    }

    // EventMslGenMiss 0301
    if (action.type === "0301") {
      playerState.missilesLeft -= 1;
    }

    // EventMslGenDestroy 0303
    if (action.type === "0303") {
      playerState.missilesLeft -= 1;
      playerState.destroyBase += 1;
      playerState.missileBase += 1;
      playerState.score += 1001;
      playerState.spEarned += 5;
    }

    // EventMslOppDown 0306
    if (action.type === "0306") {
      playerState.missilesLeft -= 1;
      if (player.team === target.team) {
        playerState.score -= 500;
        playerState.deacTeam += 1;
        playerState.missileTeam += 1;
      } else {
        playerState.score += 500;
        playerState.deacOpponent += 1;
        playerState.missileOpponent += 1;
        playerState.spEarned += 5;
      }

      if (
        target.position === EntityType.Commander ||
        target.position === EntityType.Heavy
      ) {
        playerState.deac3Hit += 1;
      }
      if (target.position === EntityType.Medic) {
        playerState.medicHits += 2;
      }

      targetState.score -= 100;
      targetState.isActive = false;
      targetState.lives = Math.max(targetState.lives - 2, 0);
      targetState.currentHP = target.maxHP;
      targetState.lastDeacTime = action.time;
      if (player.team === target.team) {
        targetState.lastDeacType = DeacType.Team;
      } else {
        targetState.lastDeacType = DeacType.Opponent;
      }
      targetState.selfDeac += 1;
      targetState.selfMissile += 1;
      if (targetState.isRapidActive) {
        targetState.selfDeacDuringRapid += 1;
        targetState.selfMissileDuringRapid += 1;
      }
      if (targetState.isNuking) {
        targetState.isNuking = false;
        if (player.team === target.team) {
          targetState.ownNukeCanceledByTeam += 1;
          playerState.cancelTeamNuke += 1;
        } else {
          targetState.ownNukeCanceledByOpponent += 1;
          playerState.cancelOpponentNuke += 1;
        }
      }
    }

    //track rapid fire starts
    if (action.type === "0400") {
      playerState.isRapidActive = true;
      playerState.spSpent += 10;
    }

    if (action.type === "0404") {
      playerState.isNuking = true;
      playerState.spSpent += 20;
      playerState.nukesActivated += 1;
    }

    if (action.type === "0405") {
      playerState.isNuking = false;
      playerState.nukesDetonated += 1;
      playerState.score += 500;

      for (const [ipl_id, state] of currentState.entries()) {
        let p = entities.get(ipl_id) as Entity;
        if (p.team !== player.team) {
          if (p.position === EntityType.Medic) {
            playerState.nukeMedicHits += Math.min(state.lives, 3);
          }
          if (state.isNuking) {
            state.isNuking = false;
            state.ownNukeCanceledByNuke += 1;
            playerState.cancelOpponentNuke += 1;
          }
          state.isActive = false;
          state.lastDeacTime = action.time;
          state.lastDeacType = DeacType.Nuke;
          state.lives = Math.max(state.lives - 3, 0);
          state.currentHP = p.maxHP;
        }
      }
    }

    // EventResupplyShots 0500
    if (action.type === "0500") {
      playerState.shotsFired += 1;
      playerState.resupplyShots += 1;

      targetState.isActive = false;
      targetState.lastDeacTime = action.time;
      targetState.lastDeacType = DeacType.Resupply;
      targetState.selfResupplyShots += 1;
      targetState.shots = Math.min(
        targetState.shots + target.resupplyShots,
        target.maxShots
      );
      if (targetState.isNuking) {
        targetState.isNuking = false;
        targetState.ownNukeCanceledByTeam += 1;
        targetState.ownNukeCanceledByResupply += 1;
        playerState.cancelTeamNukeByResupply += 1;
        playerState.cancelTeamNuke += 1;
      }
    }
    // EventResupplyLives 0502
    if (action.type === "0502") {
      playerState.shotsFired += 1;
      playerState.resupplyLives += 1;

      targetState.isActive = false;
      targetState.lastDeacTime = action.time;
      targetState.lastDeacType = DeacType.Resupply;
      targetState.selfResupplyLives += 1;
      targetState.lives = Math.min(
        targetState.lives + target.resupplyLives,
        target.maxLives
      );
      //One goddamn time, DK
      if (targetState.isNuking) {
        targetState.isNuking = false;
        targetState.ownNukeCanceledByTeam += 1;
        targetState.ownNukeCanceledByResupply += 1;
        playerState.cancelTeamNukeByResupply += 1;
        playerState.cancelTeamNuke += 1;
      }
    }

    // EventResupplyTeamShots 0510
    if (action.type === "0510") {
      playerState.ammoBoosts += 1;
      playerState.spSpent += 15;

      for (const [ipl_id, state] of currentState.entries()) {
        let p = entities.get(ipl_id) as Entity;
        if (
          p.team === player.team &&
          state.isActive &&
          p.position !== EntityType.Ammo
        ) {
          state.shots = Math.min(state.shots + p.resupplyShots, p.maxShots);
          state.ammoBoostReceieved += 1;
          playerState.ammoBoostedPlayers += 1;
        }
      }
    }

    // EventResupplyTeamLives 0512
    if (action.type === "0512") {
      playerState.lifeBoosts += 1;
      playerState.spSpent += 10;

      for (const [ipl_id, state] of currentState.entries()) {
        let p = entities.get(ipl_id) as Entity;
        if (
          p.team === player.team &&
          state.isActive &&
          p.position !== EntityType.Medic
        ) {
          state.lives = Math.min(state.lives + p.resupplyLives, p.maxLives);
          state.lifeBoostReceived += 1;
          playerState.lifeBoostedPlayers += 1;
        }
      }
    }

    // EventPenalty 0600
    if (action.type === "0600") {
      playerState.isActive = false;
      playerState.penalties += 1;
      if (targetState.isNuking) {
        targetState.isNuking = false;
        targetState.ownNukeCanceledByPenalty += 1;
      }
    }

    // EventReactivate LFS001
    if (action.type === "LFS001") {
      playerState.isActive = true;
    }

    action.state = _.cloneDeep(currentState);
  }

  for (let entity of entities.values()) {
    if (entity.type === "player") {
      entity.finalState = _.cloneDeep(
        currentState.get(entity.ipl_id)
      ) as EntityState;
      entity.finalState.isFinal = true;
    }
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
        entities,
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
