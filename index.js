const aws = require("aws-sdk");
const readline = require("readline");
const moment = require("moment");
const iconv = require("iconv-lite");
const AutoDetectDecoderStream = require("autodetect-decoder-stream");
const s3 = new aws.S3({ apiVersion: "2006-03-01" });
const { Pool } = require("pg");

const targetBucket = process.env.TARGET_BUCKET;
const connectionString = process.env.DATABASE_URL;

const pool = new Pool({
  connectionString: connectionString
});

exports.handler = async event => {
  //console.log("Received event:", JSON.stringify(event, null, 2));

  const bucket = event.Records[0].s3.bucket.name;
  const key = decodeURIComponent(
    event.Records[0].s3.object.key.replace(/\+/g, " ")
  );

  const params = {
    Bucket: bucket,
    Key: key
  };

  const s3ReadStream = s3
    .getObject(params)
    .createReadStream()
    .pipe(new AutoDetectDecoderStream())
    .pipe(iconv.encodeStream("utf8"));

  const rl = readline.createInterface({
    input: s3ReadStream,
    terminal: false
  });

  const ENTITY_TYPES = {
    1: "Commander",
    2: "Heavy Weapons",
    3: "Scout",
    4: "Ammo Carrier",
    5: "Medic"
  };

  var entities = new Map();
  var teams = new Map();
  var game = {};
  var metadata = {};
  var actions = [];
  var score_deltas = [];

  let chompFile = new Promise((resolve, reject) => {
    rl.on("line", line => {
      let record = line.split("\t");
      if (record[0].includes(';"')) {
        return;
      } else {
        if (record[0] == 0) {
          //;0/version	file-version	program-version
          metadata = {
            file_version: record[1],
            program_version: record[2]
          };
          game = {
            center: record[3]
          };
        } else if (record[0] == 1) {
          //;1/mission	type	desc	start
          game = {
            type: record[1],
            desc: record[2],
            start: parseInt(record[3]),
            starttime: moment(record[3], "YYYYMMDDHHmmss").format(),
            ...game
          };
        } else if (record[0] == 2) {
          //;2/team	index	desc	colour-enum	colour-desc
          //normalize the team colors to either red or green because reasons
          let normal_team = "";
          if (record[4] == "Fire" || record[4] == "Red") {
            normal_team = "red";
          } else if (
            record[4] == "Ice" ||
            record[4] == "Yellow" ||
            record[4] == "Blue" ||
            record[4] == "Green" ||
            record[4] == "Earth"
          ) {
            normal_team = "green";
          }

          let team = {
            index: record[1],
            desc: record[2],
            color_enum: record[3],
            color_desc: record[4],
            score: 0,
            livesLeft: 0,
            normal_team: normal_team
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
            position: ENTITY_TYPES[record[7]],
            lfstats_id: null,
            hits: new Map()
          };
          entities.set(entity.ipl_id, entity);
        } else if (record[0] == 4) {
          //;4/event	time	type	varies
          let action = {
            time: record[1],
            type: record[2],
            player: record[3]
          };
          if (typeof record[4] != "undefined") action.action = record[4];
          if (typeof record[5] != "undefined") action.target = record[5];

          actions.push(action);
          //track and total hits
          if (
            record[2] == "0205" ||
            record[2] == "0206" ||
            record[2] == "0306"
          ) {
            let player = entities.get(record[3]);
            let target = entities.get(record[5]);

            if (!player.hits.has(target.ipl_id)) {
              player.hits.set(target.ipl_id, {
                ipl_id: target.ipl_id,
                hits: 0,
                missiles: 0
              });
            }

            if (record[2] == "0205" || record[2] == "0206")
              player.hits.get(target.ipl_id).hits += 1;
            if (record[2] == "0306")
              player.hits.get(target.ipl_id).missiles += 1;
          }

          //compute game start, end and length
          if (record[2] == "0101") {
            let gameLength;
            gameLength = (Math.round(record[1] / 1000) * 1000) / 1000;
            game.endtime = moment(game.start, "YYYYMMDDHHmmss")
              .seconds(gameLength)
              .format();
            game.gameLength = gameLength;
          }
        } else if (record[0] == 5) {
          //;5/score	time	entity	old	delta	new
          score_deltas.push({
            time: record[1],
            player: record[2],
            old: record[3],
            delta: record[4],
            new: record[5]
          });
        } else if (record[0] == 6) {
          //;6/entity-end	time	id	type	score
          let player = entities.get(record[2]);
          player = {
            end: parseInt(record[1]),
            score: parseInt(record[4]),
            survived:
              (Math.round((record[1] - player.start) / 1000) * 1000) / 1000,
            ...player
          };
          entities.set(player.ipl_id, player);
        } else if (record[0] == 7) {
          //;7/sm5-stats	id	shotsHit	shotsFired	timesZapped	timesMissiled	missileHits	nukesDetonated	nukesActivated	nukeCancels	medicHits	ownMedicHits	medicNukes	scoutRapid	lifeBoost	ammoBoost	livesLeft	shotsLeft	penalties	shot3Hit	ownNukeCancels	shotOpponent	shotTeam	missiledOpponent	missiledTeam
          let player = entities.get(record[1]);
          player = {
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
            ...player
          };
          teams.get(player.team).livesLeft += player.livesLeft;
          teams.get(player.team).score += player.score;
        }
      }
    });
    rl.on("error", () => {
      console.log("read error");
    });
    rl.on("close", async () => {
      console.log("done reading");
      resolve();
    });
  });

  try {
    await chompFile;

    const storageParams = {
      CopySource: bucket + "/" + key,
      Bucket: targetBucket,
      Key: `${game.center}_${game.start}_${game.desc.replace(/ /g, "-")}.tdf`
    };

    await s3
      .copyObject(storageParams, function(err, data) {
        if (err) console.log(err, err.stack);
        // an error occurred
        else console.log("Put TDF file", data); // successful response
      })
      .promise();
  } catch (err) {
    console.log("Error", err.stack);
  }

  //IMPORT PROCESS

  //cehck for game center id and timestamp - if exists, abort
  //insert game and get id
  //insaert the teams objects as well into a column on game - maybe use it in the future

  //insert scorecards with player id and game_id
  //will default to evnet_id NULL whihc will put them in the review queue

  //insert hit stats
  //insert missile stats

  //insert game actions and scorecard delta

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    //Let's see if the game already exists before we start doing too much
    let gameCount = await client.query(
      "select games.id from games inner join centers on games.center_id=centers.id where game_datetime=$1 and centers.ipl_id=$2",
      [game.starttime, game.center]
    );

    if (gameCount.rowCount == 0) {
      let center = await client.query("SELECT * from centers WHERE ipl_id=$1", [
        game.center
      ]);

      //find or create lfstats player IDs
      //let's get or create our player instances
      //roll through the entities
      //cehck for IPL match first, if exists, get id and move to next
      //check for name && ipl_id is null - udpate ipl_id if found and get id
      //create new player record
      //NON IPL ENTITIES?????? - NULL player_id? lots of side effects but maybe best option - would allow deleting a lot of DB cruft
      //null player_id would fuck up hits though
      //maybe just update wth @ id
      entities.forEach(async (player, key, map) => {
        if (player.type == "player") {
          let playerRecord = await client.query(
            "SELECT * FROM players where ipl_id=$1",
            [player.ipl_id]
          );
          if (playerRecord.rowCount > 0) {
            //IPL exists, let's save the lfstats id...yes we already wrote output to file, this is jsut for convenience
            player.lfstats_id = playerRecord.rows[0].id;
            //Is the player using a new alias?
            let playerNames = await client.query(
              "SELECT * FROM players_names WHERE players_names.player_id=$1 AND players_names.player_name=$2",
              [player.lfstats_id, player.desc]
            );
            if (playerNames.rowCount == 0) {
              //this is a new alias! why do people do this. i've used one name since 1997. commit, people.
              //let's set all other names to inactive
              await client.query(
                "UPDATE players_names SET is_active=false WHERE player_id=$1",
                [player.lfstats_id]
              );
              //insert the new alias and make it active
              await client.query(
                "INSERT INTO players_names (player_id,player_name,is_active) VALUES ($1, $2, true)",
                [player.lfstats_id, player.desc]
              );
            }
          } else {
            //IPL doesn't exist, so let's see if this player name already exists and tie the IPL to an existing record
            //otherwise create a BRAND NEW player
            let existingPlayer = await client.query(
              "SELECT * FROM players_names WHERE player_name=$1",
              [player.desc]
            );
            if (existingPlayer.rowCount > 0) {
              //Found a name, let's use it
              player.lfstats_id = existingPlayer.rows[0].player_id;
              await client.query("UPDATE players SET ipl_id=$1 WHERE id=$2", [
                player.ipl_id,
                player.lfstats_id
              ]);
            } else {
              //ITS A FNG
              let newPlayer = await client.query(
                "INSERT INTO players (player_name,ipl_id) VALUES ($1,$2) RETURNING ID",
                [player.desc, player.ipl_id]
              );
              player.lfstats_id = newPlayer.rows[0].id;
              await client.query(
                "INSERT INTO players_names (player_id,player_name,is_active) VALUES ($1, $2, true)",
                [player.lfstats_id, player.desc]
              );
            }
          }
        }
        map.set(key, player);
      });

      //start working on game details pre-insert
      //need to normalize team colors and determine elims before inserting the game
      let redTeam;
      let greenTeam;
      for (const [key, value] of teams) {
        if (value.normal_team == "red") redTeam = value;
        if (value.normal_team == "green") greenTeam = value;
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

      const insertGameQuery = {
        text:
          "INSERT INTO games (game_name,game_description,game_datetime,game_length,red_score,green_score,red_adj,green_adj,winner,red_eliminated,green_eliminated,type,center_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING id",
        values: [
          "Game @ " + game.starttime,
          "",
          game.starttime,
          game.gameLength,
          redTeam.score,
          greenTeam.score,
          redBonus,
          greenBonus,
          winner,
          redElim,
          greenElim,
          "social",
          center.rows[0].id
        ]
      };
      let gameRecord = await client.query(insertGameQuery);

      const insertGameActionsQueryText =
        "INSERT INTO game_actions(action_time, action, game_id) VALUES($1, $2, $3) RETURNING id";

      for (let action of actions) {
        if (entities.has(action.player)) {
          action.player_id = entities.get(action.player).lfstats_id;
        }
        if (entities.has(action.target)) {
          action.target_id = entities.get(action.target).lfstats_id;
        }
        await client.query(insertGameActionsQueryText, [
          action.time,
          action,
          gameRecord.rows[0].id
        ]);
      }
    }

    await client.query("COMMIT");
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }

  console.log("CHOMP COMPLETE");
};
