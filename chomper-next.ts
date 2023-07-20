import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { APIGatewayEvent, APIGatewayProxyResult } from "aws-lambda";
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import { v4 as uuidv4 } from "uuid";
import { once } from "events";
import { createInterface } from "readline";
import { Readable } from "stream";
import { decodeStream, encodeStream } from "iconv-lite";
import { DateTime } from "luxon";
import { createPool, sql } from "slonik";
import { createQueryLoggingInterceptor } from "slonik-interceptor-query-logging";
import { cloneDeep, isEqual } from "lodash";
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
  chomperVersion,
  defaultInitialState,
  entityTypes,
  positionDefaults,
  UIColors,
  DefaultMVPModel,
} from "./constants";
import generateMVP from "./generateMVP";

export const version = async (
  event: APIGatewayEvent
): Promise<APIGatewayProxyResult> => {
  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        chomperVersion: chomperVersion,
      },
      null,
      2
    ),
  };
};

export const chomper = async (
  event: APIGatewayEvent
): Promise<APIGatewayProxyResult> => {
  const tdfId = event.queryStringParameters?.tdfId;
  let gameId: number = 0;
  const interceptors = [createQueryLoggingInterceptor()];

  if (!tdfId) {
    return {
      statusCode: 400,
      body: JSON.stringify(
        {
          message: "No TDF ID Provided",
        },
        null,
        2
      ),
    };
  }

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
      connectionString = `postgres://${secret.username}:${secret.password}@${secret.host}:${secret.port}/lfstats_tdf?sslmode=require`;
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
  console.log("CHOMP: READING " + tdfId);

  //object to store our results of parsing
  let game: Game = {
    lfstatsId: null,
    missionType: "",
    missionDesc: "",
    missionStart: 0,
    missionStartTime: "",
    missionMaxLength: 0,
    missionMaxLengthMillis: 0,
    missionLength: 900,
    missionLengthMillis: 900000,
    penaltyValue: null,
  };
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
          game.missionType = record[1];
          game.missionDesc = record[2];
          game.missionStart = parseInt(record[3]);
          game.missionStartTime = DateTime.fromFormat(
            record[3],
            "yyyyMMddHHmmss",
            {
              zone: "utc",
            }
          ).toSQL({ includeOffset: false });
          game.missionMaxLength = record[4]
            ? (Math.round(parseInt(record[4]) / 1000) * 1000) / 1000
            : 900;
          game.missionMaxLengthMillis =
            typeof record[4] != "undefined" ? parseInt(record[4]) : 900000;

          game.penaltyValue =
            typeof record[5] != "undefined" ? parseInt(record[5]) : null;
        } else if (record[0] === "2") {
          //;2/team	index	desc	colour-enum	colour-desc
          let team = {
            index: parseInt(record[1]),
            desc: record[2],
            colorEnum: parseInt(record[3]),
            colorDesc: record[4],
            uiColor: UIColors[parseInt(record[3])],
            lfstatsId: null,
            isEliminated: false,
            oppEliminated: false,
            elimBonus: 0,
          } as Team;
          teams.set(team.index, team);
        } else if (record[0] === "3") {
          //;3/entity-start	time	id	type	desc	team	level	category
          let position = entityTypes[parseInt(record[7])] ?? null;
          let entity = {
            startTime: parseInt(record[1]),
            iplId: record[2],
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
          entity.initialState.uuid = uuidv4();
          entity.initialState.position = position;
          entity.initialState.iplId = entity.iplId;
          entity.initialState.shots = entity.initialShots ?? 0;
          entity.initialState.lives = entity.initialLives ?? 0;
          entity.initialState.missilesLeft = entity.initialMissiles ?? 0;
          entity.initialState.currentHP = entity.maxHP ?? 0;

          entities.set(entity.iplId, entity);
          currentState.set(entity.iplId, { ...entity.initialState });
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
          player.endTime = parseInt(record[1]);
          player.endCode = record[3];
        } else if (record[0] === "7") {
          //do nothing for the moment - maybe add some validation later
          //lol yeah right
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
  } catch (error: any) {
    console.log("CHOMP: READ ERROR");
    const { requestId, cfId, extendedRequestId } = error.$metadata;
    //console.log({ requestId, cfId, extendedRequestId, error });
    console.log({ requestId, cfId, extendedRequestId });
    return {
      statusCode: 502,
      body: JSON.stringify(
        {
          message: "TDF Read Error",
        },
        null,
        2
      ),
    };
  }

  //add a synthetic eliminated action so we can update state correctly
  let elimActions: GameAction[] = [];
  for (let entity of entities.values()) {
    if (entity.type === "player" && entity.endCode !== "02") {
      elimActions.push({
        time: entity.endTime as number,
        type: "LFS002",
        action: " eliminated",
        player: entity.iplId,
        target: null,
        state: null,
      });
    }
  }
  actions = [...actions, ...elimActions];
  console.log("CHOMP: CREATED ELIM ACTIONS");

  //set team elim states
  for (let team of teams.values()) {
    if (team.colorEnum !== 0) {
      team.isEliminated = [...entities]
        .filter(
          ([, entity]) => entity.team === team.index && entity.type === "player"
        )
        .every(([, entity]) => entity.endCode === "04");
    }
  }
  console.log("CHOMP: SET TEAM ELIM STATES");

  //set opp elim sates and bonus
  for (let team of teams.values()) {
    if (team.colorEnum !== 0) {
      team.oppEliminated = [...teams]
        .filter(([, t]) => t.index !== team.index && t.colorEnum !== 0)
        .every(([, t]) => t.isEliminated);
      if (team.oppEliminated) team.elimBonus = 10000;
    }
  }

  //make sure our actions array is in time slice order
  actions.sort((a, b) => {
    return a.time - b.time;
  });

  console.log("CHOMP: ELIM ACTIONS COMPLETE");

  let reacActions: GameAction[] = [];
  //initialize entity state for creating reac events
  //this jsut holds an IplID and a lastdeactime - which will be null if the player is online
  let tempStates = new Map<string, any>();
  for (let entity of entities.values()) {
    if (entity.type === "player") {
      tempStates.set(entity.iplId, {
        lastDeacTime: null,
        endTime: entity.endTime,
      });
    }
  }

  //for any deac event, set or update the target's last deac time
  //a nuke resets it for all opposing team players
  for (let action of actions) {
    for (let [IplId, state] of tempStates) {
      if (
        state.lastDeacTime &&
        state.lastDeacTime + 8000 <= action.time &&
        action.time < state.endTime
      ) {
        reacActions.push({
          time: state.lastDeacTime + 8000,
          type: "LFS001",
          action: " reactivated",
          player: IplId,
          target: null,
          state: null,
        });
        state.lastDeacTime = null;
        tempStates.set(IplId, state);
      }
    }
    if (
      action.type === "0206" ||
      action.type === "0306" ||
      action.type === "0500" ||
      action.type === "0502"
    ) {
      let t = tempStates.get(action.target as string);
      t.lastDeacTime = action.time;
      tempStates.set(action.target as string, t);
    }
    //penalties are different beacause reasons
    if (action.type === "0600") {
      let t = tempStates.get(action.player as string);
      t.lastDeacTime = action.time;
      tempStates.set(action.player as string, t);
    }
    if (action.type === "0405") {
      //nuke
      let player = entities.get(action.player as string) as Entity;
      for (let [IplId, target] of entities) {
        if (target.type === "player" && player.team !== target.team) {
          let t = tempStates.get(IplId as string);
          t.lastDeacTime = action.time;
          tempStates.set(IplId as string, t);
        }
      }
    }
  }

  actions = [...actions, ...reacActions];
  actions.sort((a, b) => {
    return a.time - b.time;
  });

  console.log("CHOMP: REAC ACTIONS COMPLETE");

  //now let's see about inserting assists
  //create a new map of entities to track the last time they were tagged
  //but onyl need to include heavies and commanders
  let assistCandidates = new Map<string, any>();
  let assistActions: GameAction[] = [];
  for (let entity of entities.values()) {
    if (
      entity.position === EntityType.Commander ||
      entity.position === EntityType.Heavy
    ) {
      assistCandidates.set(entity.iplId, {
        assists: [] as GameAction[],
      });
    }
  }

  //iterate through actions and look for 0205 events acted by the opposing team
  //when we find one set the last tag time
  //then for any 0206 events
  for (let action of actions) {
    if (action.type === "0205") {
      let player = entities.get(action.player as string) as Entity;
      let target = entities.get(action.target as string) as Entity;
      if (player.team !== target.team) {
        //possible assist!
        let assist: GameAction = {
          time: action.time,
          type: "LFS003",
          action: " assists vs ",
          player: player.iplId,
          target: target.iplId,
          state: null,
        };
        //have to avoid duplicate assists
        let unique = true;
        for (let existingAssist of assistCandidates.get(target.iplId).assists) {
          if (assist.player === existingAssist.player) {
            unique = false;
            break;
          }
        }
        if (unique) assistCandidates.get(target.iplId).assists.push(assist);
      }
    }

    if (action.type === "0206") {
      let target = entities.get(action.target as string) as Entity;
      let assists = assistCandidates.get(target.iplId)?.assists ?? null;
      if (assists) {
        while (assists.length) {
          let assist = assists.pop();
          if (
            action.player !== assist.player &&
            action.time - assist.time <= 4000
          ) {
            //the potential assist is within 4 seconds!
            assist.time = action.time;
            assistActions.push(assist);
          }
        }
      }
    }
  }
  actions = [...actions, ...assistActions];
  actions.sort((a, b) => {
    return a.time - b.time;
  });

  console.log("CHOMP: ASSIST ACTIONS COMPLETE");

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
  // EventMslOwnDown 0308
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
  // EventBaseAwarded 0B03

  //initialize history with all our start states
  let stateHistory: EntityState[] = [];
  for (const [ipl_id, state] of currentState.entries()) {
    let p = entities.get(ipl_id) as Entity;
    if (p.type === "player")
      pushState(state, null, stateHistory, game, teams.get(p.team) as Team);
  }
  console.log("CHOMP: STATE HISTORY INIT");

  //Just an absolute shit show of naive code and duplication
  //However, it makes it clean to see exactly how each action is mutating state
  //Would be good to some day pull this into separate mutator type functions
  for (let action of actions) {
    //First we'll iterate through actions that only have a player - no target
    if (action.player) {
      let playerState = currentState.get(action.player) as EntityState;
      let player = entities.get(action.player) as Entity;
      let prevPlayerState = cloneDeep(playerState);

      // EventShotMiss 0201
      // EventShotGenMiss 0202
      if (action.type === "0201" || action.type === "0202") {
        playerState.stateTime = action.time;
        playerState.shots =
          player.position === EntityType.Ammo
            ? player.initialShots
            : playerState.shots - 1;
        playerState.shotsFired += 1;
        if (playerState.isRapid) {
          playerState.shotsFiredDuringRapid += 1;
        }
      }

      // EventShotGenDamage 0203
      // EventShotGenDestroy 0204
      if (action.type === "0203" || action.type === "0204") {
        playerState.stateTime = action.time;
        playerState.shots =
          player.position === EntityType.Ammo
            ? player.initialShots
            : playerState.shots - 1;
        playerState.shotsFired += 1;
        playerState.shotsHit += 1;
        if (playerState.isRapid) {
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

      // EventMslGenMiss 0301
      if (action.type === "0301") {
        playerState.stateTime = action.time;
        playerState.missilesLeft -= 1;
      }

      // EventMslGenDestroy 0303
      if (action.type === "0303") {
        playerState.stateTime = action.time;
        playerState.missilesLeft -= 1;
        playerState.destroyBase += 1;
        playerState.missileBase += 1;
        playerState.score += 1001;
        playerState.spEarned += 5;
      }

      // EventMslGenDestroy 0B03
      if (action.type === "0B03") {
        playerState.stateTime = action.time;
        playerState.awardBase += 1;
        playerState.score += 1001;
      }

      //track rapid fire starts
      if (action.type === "0400") {
        playerState.stateTime = action.time;
        playerState.rapidFires += 1;
        playerState.isRapid = true;
        playerState.spSpent += 10;
      }

      if (action.type === "0404") {
        playerState.stateTime = action.time;
        playerState.isNuking = true;
        playerState.spSpent += 20;
        playerState.nukesActivated += 1;
      }

      if (action.type === "0405") {
        playerState.stateTime = action.time;
        playerState.isNuking = false;
        playerState.nukesDetonated += 1;
        playerState.score += 500;

        for (const [ipl_id, state] of currentState.entries()) {
          let prevState = cloneDeep(state);
          let p = entities.get(ipl_id) as Entity;
          if (
            p.type === "player" &&
            p.team !== player.team &&
            !state.isEliminated
          ) {
            state.stateTime = action.time;
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

            pushState(
              state,
              prevState,
              stateHistory,
              game,
              teams.get(p.team) as Team
            );
          }
        }
      }

      // EventResupplyTeamShots 0510
      if (action.type === "0510") {
        playerState.stateTime = action.time;
        playerState.ammoBoosts += 1;
        playerState.spSpent += 15;

        for (const [ipl_id, state] of currentState.entries()) {
          let prevState = cloneDeep(state);
          let p = entities.get(ipl_id) as Entity;
          if (
            p.type === "player" &&
            p.team === player.team &&
            state.isActive &&
            p.position !== EntityType.Ammo
          ) {
            state.stateTime = action.time;
            state.shots = Math.min(state.shots + p.resupplyShots, p.maxShots);
            state.ammoBoostReceived += 1;
            playerState.ammoBoostedPlayers += 1;

            pushState(
              state,
              prevState,
              stateHistory,
              game,
              teams.get(p.team) as Team
            );
          }
        }
      }

      // EventResupplyTeamLives 0512
      if (action.type === "0512") {
        playerState.stateTime = action.time;
        playerState.lifeBoosts += 1;
        playerState.spSpent += 10;

        for (const [ipl_id, state] of currentState.entries()) {
          let prevState = cloneDeep(state);
          let p = entities.get(ipl_id) as Entity;
          if (
            p.type === "player" &&
            p.team === player.team &&
            state.isActive &&
            p.position !== EntityType.Medic
          ) {
            state.stateTime = action.time;
            state.lives = Math.min(state.lives + p.resupplyLives, p.maxLives);
            state.lifeBoostReceived += 1;
            playerState.lifeBoostedPlayers += 1;

            pushState(
              state,
              prevState,
              stateHistory,
              game,
              teams.get(p.team) as Team
            );
          }
        }
      }

      // EventPenalty 0600
      if (action.type === "0600") {
        playerState.stateTime = action.time;
        playerState.isActive = false;
        playerState.penalties += 1;
        if (playerState.isNuking) {
          playerState.isNuking = false;
          playerState.ownNukeCanceledByPenalty += 1;
        }
      }

      // EventReactivate LFS001
      if (action.type === "LFS001") {
        playerState.stateTime = action.time;
        playerState.isActive = true;
      }

      // EventEliminated LFS002
      if (action.type === "LFS002") {
        playerState.stateTime = action.time;
        playerState.isActive = false;
        playerState.isEliminated = true;
      }

      // EventAssist LFS003
      if (action.type === "LFS003") {
        playerState.stateTime = action.time;
        playerState.assists += 1;

        if (playerState.isRapid) {
          playerState.assistsDuringRapid += 1;
        }
      }

      if (action.target) {
        //now for actions with a player and a target
        let targetState = currentState.get(action.target) as EntityState;
        let target = entities.get(action.target) as Entity;
        let prevTargetState = cloneDeep(targetState);

        // EventShotOppDamage 0205
        // Only occurs against a 3-hit
        // can be opponent or teammate
        if (action.type === "0205") {
          playerState.stateTime = action.time;
          playerState.shots =
            player.position === EntityType.Ammo
              ? player.initialShots
              : playerState.shots - 1;
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

          if (playerState.isRapid) {
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

          targetState.stateTime = action.time;
          targetState.selfHit += 1;
          targetState.currentHP -= player.shotPower;
          targetState.score -= 20;
        }

        // EventShotOppDown 0206
        if (action.type === "0206") {
          playerState.stateTime = action.time;
          playerState.shots =
            player.position === EntityType.Ammo
              ? player.initialShots
              : playerState.shots - 1;
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

          if (playerState.isRapid) {
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
            if (playerState.isRapid) {
              playerState.shot3HitDuringRapid += 1;
              playerState.deac3HitDuringRapid += 1;
            }
          }

          if (target.position === EntityType.Medic) {
            if (player.team === target.team) {
              playerState.ownMedicHits += 1;
            } else {
              playerState.medicHits += 1;
              if (playerState.isRapid) {
                playerState.medicHitsDuringRapid += 1;
              }
            }
          }

          targetState.stateTime = action.time;
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
          if (targetState.isRapid) {
            targetState.selfHitDuringRapid += 1;
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

        // EventMslOppDown 0306
        // We have a separate event for team missile down, but leaving this in for legacy handling
        if (action.type === "0306") {
          playerState.stateTime = action.time;
          playerState.missilesLeft -= 1;
          if (player.team === target.team) {
            playerState.score -= 500;
            playerState.deacTeam += 1;
            playerState.missileTeam += 1;
          } else {
            playerState.score += 500;
            playerState.deacOpponent += 1;
            playerState.missileOpponent += 1;
            playerState.spEarned += 2;
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

          targetState.stateTime = action.time;
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
          if (player.team === target.team) {
            targetState.selfTeamMissile += 1;
          } else {
            targetState.selfMissile += 1;
          }
          if (targetState.isRapid) {
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

        // EventMslOppDown 0308
        if (action.type === "0308") {
          playerState.stateTime = action.time;
          playerState.missilesLeft -= 1;
          playerState.score -= 500;
          playerState.deacTeam += 1;
          playerState.missileTeam += 1;

          targetState.stateTime = action.time;
          targetState.score -= 100;
          targetState.isActive = false;
          targetState.lives = Math.max(targetState.lives - 2, 0);
          targetState.currentHP = target.maxHP;
          targetState.lastDeacTime = action.time;
          targetState.lastDeacType = DeacType.Team;
          targetState.selfDeac += 1;
          targetState.selfTeamMissile += 1;

          if (targetState.isRapid) {
            targetState.selfDeacDuringRapid += 1;
            targetState.selfMissileDuringRapid += 1;
          }
          if (targetState.isNuking) {
            targetState.isNuking = false;
            targetState.ownNukeCanceledByTeam += 1;
            playerState.cancelTeamNuke += 1;
          }
        }

        // EventResupplyShots 0500
        if (action.type === "0500") {
          playerState.stateTime = action.time;
          playerState.shotsFired += 1;
          playerState.shotsHit += 1;
          playerState.resupplyShots += 1;

          targetState.stateTime = action.time;
          targetState.isActive = false;
          targetState.isRapid = false;
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
          playerState.stateTime = action.time;
          playerState.shotsFired += 1;
          playerState.shotsHit += 1;
          playerState.resupplyLives += 1;

          targetState.stateTime = action.time;
          targetState.isActive = false;
          targetState.isRapid = false;
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
        pushState(
          targetState,
          prevTargetState,
          stateHistory,
          game,
          teams.get(target.team) as Team
        );
      }
      pushState(
        playerState,
        prevPlayerState,
        stateHistory,
        game,
        teams.get(player.team) as Team
      );
    }

    action.state = cloneDeep(currentState);
  }

  console.log("CHOMP: ACTIONS COMPLETE");

  //set final states
  for (let [, state] of currentState) {
    let prevState = cloneDeep(state);
    let entity = entities.get(state.iplId) as Entity;
    state.stateTime = entity.endTime as number;
    state.isFinal = true;
    if (entity.type === "player") {
      if (state.isNuking) {
        state.isNuking = false;
        state.ownNukeCanceledByGameEnd += 1;
      }
      pushState(
        state,
        prevState,
        stateHistory,
        game,
        teams.get(entity.team) as Team
      );
    }
  }

  console.log("CHOMP: FINAL STATES COMPLETE");

  try {
    const pool = await createPool(connectionString, { interceptors });
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
                .filter(([, e]) => e.type === "player")
                .sort()
                .map(([, e]) => sql.join([e.iplId, e.desc], sql`, `)),
              sql`), (`
            )}
          )
          ON CONFLICT (ipl_id) DO UPDATE SET current_alias = EXCLUDED.current_alias
          RETURNING *
        `);

        //update our entities with their lfstats IDs for future reference
        for (let player of playerRecords) {
          let entity = entities.get(player.ipl_id as string);
          if (entity) {
            entity.lfstatsId = player.id as number;
          }
        }

        //insert current aliases
        await connection.many(sql`
          INSERT INTO player_alias (alias,last_used,player_id)
          VALUES (
            ${sql.join(
              [...entities]
                .filter(([, p]) => p.type === "player")
                .sort()
                .map(([, p]) =>
                  sql.join(
                    [p.desc, game.missionStartTime, p.lfstatsId],
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
        let gameExist = await client.maybeOne(
          sql`SELECT id, chomper_version
            FROM game 
            WHERE mission_start=${game.missionStartTime} AND center_id=${centerRecord.id}`
        );

        if (gameExist) {
          if (gameExist.chomper_version !== chomperVersion) {
            // the game exists in the database but doesn't match the current chomper version
            // delete and rebuild
            console.log("CHOMP2 REBUILD: game exists with old chomper");
            await client.query(sql`
            DELETE FROM game WHERE id=${gameExist.id} 
            `);
          } else {
            console.log("CHOMP2 ABORTED: game exists");
            throw "GameExistsError";
          }
        }

        console.log("CHOMP2 STATUS: Create Game Record");

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
                chomper_version,
                tdf_id
              )
            VALUES
              (
                ${game.missionType},
                ${game.missionDesc},
                ${game.missionStartTime},
                ${game.missionMaxLengthMillis},
                ${game.penaltyValue},
                ${game.missionLengthMillis},
                ${centerRecord.id},
                ${gameMetaData.fileVersion},
                ${gameMetaData.programVersion},
                ${gameMetaData.chomperVersion},
                ${tdfId}
              )
            RETURNING *
          `);
        gameId = gameRecord.id as number;

        console.log("CHOMP2 STATUS: Add Default Tag");

        //For now, assuming all games are Social and applying the global Social tag (id 1)
        await client.query(
          sql`INSERT INTO game_tag (tag_id, game_id) VALUES (1, ${gameId})`
        );

        console.log("CHOMP2 STATUS: Create Team Records");

        //on to the teams
        let gameTeamRecords = await client.many(sql`
          INSERT INTO game_team
            (
              team_index,
              team_desc,
              color_enum,
              color_desc,
              ui_color,
              is_eliminated,
              opp_eliminated,
              elim_bonus,
              game_id
            )
          VALUES
            (
              ${sql.join(
                [...teams].map(([, t]) =>
                  sql.join(
                    [
                      t.index,
                      t.desc,
                      t.colorEnum,
                      t.colorDesc,
                      t.uiColor,
                      t.isEliminated,
                      t.oppEliminated,
                      t.elimBonus,
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

        let gameTeams: number[] = [];
        for (let gameTeamRecord of gameTeamRecords) {
          gameTeams[gameTeamRecord.team_index as number] = <number>(
            gameTeamRecord.id
          );
        }
        for (let [, entity] of entities)
          entity.gameTeamId = gameTeams[entity.team];

        console.log("CHOMP2 STATUS: Create Entity Records");

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
              end_time,
              position,
              start_time,
              player_id
            )
          VALUES 
            (
              ${sql.join(
                [...entities].map(([, e]) =>
                  sql.join(
                    [
                      e.iplId,
                      e.type,
                      e.desc,
                      e.level,
                      e.category,
                      e.battlesuit,
                      e.gameTeamId,
                      e.endCode,
                      e.endTime,
                      e.position,
                      e.startTime,
                      e.lfstatsId,
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
          let entity = entities.get(gameEntityRecord.ipl_id as string);
          if (entity) entity.lfstatsId = gameEntityRecord.id as number;
        }

        console.log("CHOMP2 STATUS: Create Action Records");

        //insert the actions
        let chunkSize = 1000;
        for (let i = 0, len = actions.length; i < len; i += chunkSize) {
          let chunk = actions.slice(i, i + chunkSize);
          await client.query(sql`
            INSERT INTO game_action
              (action_time, action_type, action_text, actor_game_entity_id, target_game_entity_id, game_id) 
            VALUES (
              ${sql.join(
                chunk.map((action) =>
                  sql.join(
                    [
                      action.time,
                      action.type,
                      action.action,
                      entities.get(action.player as string)?.lfstatsId ?? null,
                      entities.get(action.target as string)?.lfstatsId ?? null,
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

        console.log("CHOMP2 STATUS: Create State History Records");

        chunkSize = 500;
        for (let i = 0, len = stateHistory.length; i < len; i += chunkSize) {
          let chunk = stateHistory.slice(i, i + chunkSize);

          let query = sql`
          INSERT INTO game_entity_state
            (
              id,
              entity_id,
              state_time,
              is_final,
              score,
              is_active,
              is_nuking,
              is_eliminated,
              lives,
              shots,
              current_hp,
              last_deac_time,
              last_deac_type,
              is_rapid,
              shots_fired,
              shots_hit,
              shot_team,
              deac_team,
              shot_3hit,
              deac_3hit,
              shot_opponent,
              deac_opponent,
              assists,
              shot_base,
              miss_base,
              destroy_base,
              award_base,
              medic_hits,
              own_medic_hits,
              self_hit,
              self_deac,
              missile_base,
              missile_team,
              missile_opponent,
              missiles_left,
              self_missile,
              self_team_missile,
              sp_spent,
              sp_earned,
              resupply_shots,
              self_resupply_shots,
              self_resupply_lives,
              resupply_lives,
              ammo_boosts,
              life_boosts,
              ammo_boosted_players,
              life_boosted_players,
              rapid_fires,
              shots_fired_during_rapid,
              shots_hit_during_rapid,
              shot_team_during_rapid,
              deac_team_during_rapid,
              shot_3hit_during_rapid,
              deac_3hit_during_rapid,
              shot_opponent_during_rapid,
              deac_opponent_during_rapid,
              assists_during_rapid,
              medic_hits_during_rapid,
              self_hit_during_rapid,
              self_deac_during_rapid,
              self_missile_during_rapid,
              nukes_activated,
              nukes_detonated,
              nuke_medic_hits,
              own_nuke_canceled_by_nuke,
              own_nuke_canceled_by_game_end,
              own_nuke_canceled_by_team,
              own_nuke_canceled_by_resupply,
              own_nuke_canceled_by_opponent,
              own_nuke_canceled_by_penalty,
              ammo_boost_received,
              life_boost_received,
              cancel_opponent_nuke,
              cancel_team_nuke,
              cancel_team_nuke_by_resupply,
              uptime,
              resupply_downtime,
              nuke_downtime,
              team_deac_downtime,
              opp_deac_downtime,
              penalty_downtime,
              penalties
            )
          VALUES (
            ${sql.join(
              chunk.map((state) =>
                sql.join(
                  [
                    state.uuid,
                    entities.get(state.iplId)?.lfstatsId ?? null,
                    state.stateTime,
                    state.isFinal,
                    state.score,
                    state.isActive,
                    state.isNuking,
                    state.isEliminated,
                    state.lives,
                    state.shots,
                    state.currentHP,
                    state.lastDeacTime,
                    state.lastDeacType,
                    state.isRapid,
                    state.shotsFired,
                    state.shotsHit,
                    state.shotTeam,
                    state.deacTeam,
                    state.shot3Hit,
                    state.deac3Hit,
                    state.shotOpponent,
                    state.deacOpponent,
                    state.assists,
                    state.shotBase,
                    state.missBase,
                    state.destroyBase,
                    state.awardBase,
                    state.medicHits,
                    state.ownMedicHits,
                    state.selfHit,
                    state.selfDeac,
                    state.missileBase,
                    state.missileTeam,
                    state.missileOpponent,
                    state.missilesLeft,
                    state.selfMissile,
                    state.selfTeamMissile,
                    state.spSpent,
                    state.spEarned,
                    state.resupplyShots,
                    state.selfResupplyShots,
                    state.selfResupplyLives,
                    state.resupplyLives,
                    state.ammoBoosts,
                    state.lifeBoosts,
                    state.ammoBoostedPlayers,
                    state.lifeBoostedPlayers,
                    state.rapidFires,
                    state.shotsFiredDuringRapid,
                    state.shotsHitDuringRapid,
                    state.shotTeamDuringRapid,
                    state.deacTeamDuringRapid,
                    state.shot3HitDuringRapid,
                    state.deac3HitDuringRapid,
                    state.shotOpponentDuringRapid,
                    state.deacOpponentDuringRapid,
                    state.assistsDuringRapid,
                    state.medicHitsDuringRapid,
                    state.selfHitDuringRapid,
                    state.selfDeacDuringRapid,
                    state.selfMissileDuringRapid,
                    state.nukesActivated,
                    state.nukesDetonated,
                    state.nukeMedicHits,
                    state.ownNukeCanceledByNuke,
                    state.ownNukeCanceledByGameEnd,
                    state.ownNukeCanceledByTeam,
                    state.ownNukeCanceledByResupply,
                    state.ownNukeCanceledByOpponent,
                    state.ownNukeCanceledByPenalty,
                    state.ammoBoostReceived,
                    state.lifeBoostReceived,
                    state.cancelOpponentNuke,
                    state.cancelTeamNuke,
                    state.cancelTeamNukeByResupply,
                    state.uptime,
                    state.resupplyDowntime,
                    state.nukeDowntime,
                    state.teamDeacDowntime,
                    state.oppDeacDowntime,
                    state.penaltyDowntime,
                    state.penalties,
                  ],
                  sql`, `
                )
              ),
              sql`), (`
            )}
          )
        `;

          //insert the state obejcts
          await client.query(query);

          await client.query(sql`
          INSERT INTO mvp
            (
              mvp,
              mvp_details,
              mvp_model_id,
              game_entity_state_id
            )
          VALUES (
            ${sql.join(
              chunk.map((state) =>
                sql.join(
                  [
                    state.mvpValue,
                    JSON.stringify(state.mvpDetails),
                    state.mvpModelId,
                    state.uuid,
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
  } catch (error: any) {
    if (error === "GameExistsError") {
      console.log("CHOMP2: GAME EXISTS");
      return {
        statusCode: 200,
        body: JSON.stringify(
          {
            message: "Game Exists",
          },
          null,
          2
        ),
      };
    } else {
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
  }

  return {
    statusCode: 200,
    body: JSON.stringify(
      {
        message: `${tdfId} chomped successfully`,
        game_id: gameId,
      },
      null,
      2
    ),
  };
};

function calcUptime(state: EntityState, prevState: EntityState) {
  let timeDelta = state.stateTime - prevState.stateTime;
  if (prevState.isActive) {
    //Player was online prior to this event, so time delta goes to uptime
    state.uptime += timeDelta;
  } else {
    //Player was offline so time delta goes to whatever the last deac event was
    switch (prevState.lastDeacType) {
      case DeacType.Nuke:
        state.nukeDowntime += timeDelta;
        break;
      case DeacType.Opponent:
        state.oppDeacDowntime += timeDelta;
        break;
      case DeacType.Penalty:
        state.penaltyDowntime += timeDelta;
        break;
      case DeacType.Resupply:
        state.resupplyDowntime += timeDelta;
        break;
      case DeacType.Team:
        state.teamDeacDowntime += timeDelta;
        break;
    }
  }
  return state;
}

function pushState(
  state: EntityState | null,
  prevState: EntityState | null,
  stateArray: EntityState[],
  game: Game,
  team: Team
) {
  if (state) {
    state.uuid = uuidv4();

    if (state.position) {
      let mvp = generateMVP(state, DefaultMVPModel[state.position], game, team);
      state.mvpDetails = mvp.mvpDetails;
      state.mvpValue = mvp.mvpValue;
      state.mvpModelId = mvp.mvpModelId;
    }

    //if we were passed a previous state, then we need to verify its a
    //new state, then do the uptime calc
    if (prevState) {
      if (!isEqual(state, prevState))
        stateArray.push(cloneDeep(calcUptime(state, prevState)));
    } else {
      stateArray.push(cloneDeep(state));
    }
  }
}
