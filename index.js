const aws = require("aws-sdk");
const readline = require("readline");
const moment = require("moment");
const iconv = require("iconv-lite");
const AutoDetectDecoderStream = require("autodetect-decoder-stream");
const s3 = new aws.S3({ apiVersion: "2006-03-01" });
const { Pool } = require("pg");

const targetBucket = process.env.TARGET_BUCKET;
const connectionString = process.env.DATABASE_URL;

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
  var output = {};
  output.entities = {};
  output.teams = {};
  output.actions = [];
  output.score_deltas = [];

  let chompFile = new Promise((resolve, reject) => {
    rl.on("line", line => {
      let record = line.split("\t");
      if (record[0].includes(';"')) {
        return;
      } else {
        if (record[0] == 0) {
          //;0/version	file-version	program-version
          output.metadata = {
            file_version: record[1],
            program_version: record[2]
          };
          output.game = {
            center: record[3]
          };
        } else if (record[0] == 1) {
          //;1/mission	type	desc	start
          output.game = {
            type: record[1],
            desc: record[2],
            start: parseInt(record[3]),
            starttime: moment(record[3], "YYYYMMDDHHmmss").format(),
            ...output.game
          };
        } else if (record[0] == 2) {
          //;2/team	index	desc	colour-enum	colour-desc
          output.teams[record[1]] = {
            index: record[1],
            desc: record[2],
            color_enum: record[3],
            color_desc: record[4],
            score: 0
          };
        } else if (record[0] == 3) {
          //;3/entity-start	time	id	type	desc	team	level	category
          output.entities[record[2]] = {
            start: parseInt(record[1]),
            ipl_id: record[2],
            type: record[3],
            desc: record[4],
            team: parseInt(record[5]),
            level: parseInt(record[6]),
            position: ENTITY_TYPES[record[7]],
            hits: {},
            missiles: {}
          };

          //also init a hits and missiles array for this player
          if (record[3] == "player") {
            output.hits[record[2]] = {};
            output.missiles[record[2]] = {};
          }
        } else if (record[0] == 4) {
          //;4/event	time	type	varies
          output.actions.push({
            time: record[1],
            type: record[2],
            player: record[3],
            action: record[4],
            target: record[5]
          });

          //track and total hits
          if (record[2] == "0205" || record[2] == "0206") {
            if (
              typeof output.entities[record[3]].hits[record[5]] == "undefined"
            ) {
              output.entities[record[3]].hits[record[5]] = 0;
            }
            output.entities[record[3]].hits[record[5]] += 1;
          }
          //track and total missiles against players
          if (record[2] == "0306") {
            if (
              typeof output.entities[record[3]].missiles[record[5]] ==
              "undefined"
            ) {
              output.entities[record[3]].missiles[record[5]] = 0;
            }
            output.entities[record[3]].missiles[record[5]] += 1;
          }

          //compute game start, end and length
          if (record[2] == "0101") {
            let gameLength;
            gameLength = (Math.round(record[1] / 1000) * 1000) / 1000;
            output.game.endtime = moment(output.game.start, "YYYYMMDDHHmmss")
              .seconds(gameLength)
              .format();
            output.game.gameLength = gameLength;
          }
        } else if (record[0] == 5) {
          //;5/score	time	entity	old	delta	new
          output.score_deltas.push({
            time: record[1],
            player: record[2],
            old: record[3],
            delta: record[4],
            new: record[5]
          });
        } else if (record[0] == 6) {
          //;6/entity-end	time	id	type	score
          output.entities[record[2]] = {
            end: parseInt(record[1]),
            score: parseInt(record[4]),
            survived:
              (Math.round(
                (record[1] - output.entities[record[2]].start) / 1000
              ) *
                1000) /
              1000,
            ...output.entities[record[2]]
          };
        } else if (record[0] == 7) {
          //;7/sm5-stats	id	shotsHit	shotsFired	timesZapped	timesMissiled	missileHits	nukesDetonated	nukesActivated	nukeCancels	medicHits	ownMedicHits	medicNukes	scoutRapid	lifeBoost	ammoBoost	livesLeft	shotsLeft	penalties	shot3Hit	ownNukeCancels	shotOpponent	shotTeam	missiledOpponent	missiledTeam
          output.entities[record[1]] = {
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
            ...output.entities[record[1]]
          };
        }
      }
    });
    rl.on("error", () => {
      console.log("read error");
    });
    rl.on("close", function() {
      resolve();
    });
  });

  try {
    await chompFile;

    //total up team scores
    for (entity in output.entities) {
      output.teams[output.entities[entity].team].score +=
        output.entities[entity].score;
    }

    const resultParams = {
      Bucket: targetBucket,
      Key: `${output.game.center}_${
        output.game.start
      }_${output.game.desc.replace(/ /g, "-")}.json`,
      Body: JSON.stringify(output, null, 4)
    };

    const storageParams = {
      CopySource: bucket + "/" + key,
      Bucket: targetBucket,
      Key: `${output.game.center}_${
        output.game.start
      }_${output.game.desc.replace(/ /g, "-")}.tdf`
    };
    await s3
      .putObject(resultParams, function(err, data) {
        if (err) console.log(err, err.stack);
        // an error occurred
        else console.log(data); // successful response
      })
      .promise();

    await s3
      .copyObject(storageParams, function(err, data) {
        if (err) console.log(err, err.stack);
        // an error occurred
        else console.log(data); // successful response
      })
      .promise();
  } catch (err) {
    console.log("Error", err.stack);
  }

  //IMPORT PROCESS
  //roll through the entities
  //cehck for IPL match first, if exists, get id and move to next
  //check for name && ipl_id is null - udpate ipl_id if found and get id
  //create new player record
  //NON IPL ENTITIES?????? - NULL player_id? lots of side effects but maybe best option - would allow deleting a lot of DB cruft
  //null player_id would fuck up hits though
  //maybe just update wth @ id

  //if an ipl match is found, also check for aliases - if new alias on existing ipl, add alias

  //cehck for game center id and timestamp - if exists, abort
  //insert game and get id
  //insaert the teams objects as well into a column on game - maybe use it in the future

  //insert scorecards with player id and game_id
  //will default to evnet_id NULL whihc will put them in the review queue

  //insert hit stats
  //insert missile stats

  //insert game actions and scorecard delta

  //now let's go to the database
  const pool = new Pool({
    connectionString: connectionString
  });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    //Let's see if the game already exists before we start doing too much
    let res = await client.query(
      "select count(games.id) from games inner join centers on games.center_id=centers.id where game_datetime=$1 and centers.ipl_id=$2",
      [output.game.starttime, output.game.center]
    );
    if (res.rows[0].count == 0) {
      //no collision, continue with the import
      const insertGameText =
        "INSERT INTO games(game_name,game_description,game_datetime,game_length,red_score,green_score,red_adj,green_adj";

      /*const queryText =
      "INSERT INTO game_actions(action_time, action) VALUES($1, $2) RETURNING id";

    output.actions.forEach(action => {
      client.query(queryText, [action.time, action]);
    });*/
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
