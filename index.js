const aws = require("aws-sdk");
const readline = require("readline");
const moment = require("moment");
const s3 = new aws.S3({ apiVersion: "2006-03-01" });

//TODO
//Build the hits table
//Save to JSON? Or go straight to DB
//move file out of incoming

exports.handler = async (event, context) => {
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
    .createReadStream({ encoding: "utf8" });

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
  output.hits = {};
  output.missiles = {};
  output.entities = [];
  output.teams = [];
  output.events = [];
  output.score_events = [];

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
        } else if (record[0] == 1) {
          //;1/mission	type	desc	start
          console.log("date", moment(record[3], "YYYYMMDDHHmmss").format());
          output.game = {
            type: record[1],
            desc: record[2],
            start: record[3],
            starttime: moment(record[3], "YYYYMMDDHHmmss").format()
          };
        } else if (record[0] == 2) {
          //;2/team	index	desc	colour-enum	colour-desc
          output.teams.push({
            index: record[1],
            desc: record[2],
            color_enum: record[3],
            color_desc: record[4]
          });
        } else if (record[0] == 3) {
          //;3/entity-start	time	id	type	desc	team	level	category
          output.entities.push({
            start: record[1],
            ipl_id: record[2],
            type: record[3],
            desc: record[4],
            team: record[5],
            level: record[6],
            position: ENTITY_TYPES[record[7]]
          });

          //also init a hits and missiles array for this player
          if (record[3] == "player") {
            output.hits[record[2]] = {};
            output.missiles[record[2]] = {};
          }
        } else if (record[0] == 4) {
          //;4/event	time	type	varies
          output.events.push({
            time: record[1],
            type: record[2],
            player: record[3],
            action: record[4],
            target: record[5]
          });

          //track and total hits
          if (record[2] == "0205" || record[2] == "0206") {
            if (typeof output.hits[record[3]][record[5]] == "undefined") {
              output.hits[record[3]][record[5]] = 0;
            }
            output.hits[record[3]][record[5]] += 1;
          }
          //track and total missiles
          if (record[2] == "0306") {
            if (typeof output.missiles[record[3]][record[5]] == "undefined") {
              output.missiles[record[3]][record[5]] = 0;
            }
            output.missiles[record[3]][record[5]] += 1;
          }

          //compute game start, end and length
          if (record[2] == "0101") {
            gameLength = (Math.round(record[1] / 1000) * 1000) / 1000;
            output.game.endtime = moment(output.game.start, "YYYYMMDDHHmmss")
              .seconds(gameLength)
              .format();
            output.game.gameLength = gameLength;
          }
        } else if (record[0] == 5) {
          //;5/score	time	entity	old	delta	new
          output.score_events.push({
            time: record[1],
            player: record[2],
            old: record[3],
            delta: record[4],
            new: record[5]
          });
        } else if (record[0] == 6) {
          //;6/entity-end	time	id	type	score
          let index = output.entities.findIndex(obj => obj.ipl_id == record[2]);
          output.entities[index].end = record[1];
          output.entities[index].score = record[4];
          output.entities[index].survived =
            (Math.round(
              (output.entities[index].end - output.entities[index].start) / 1000
            ) *
              1000) /
            1000;
        } else if (record[0] == 7) {
          //;7/sm5-stats	id	shotsHit	shotsFired	timesZapped	timesMissiled	missileHits	nukesDetonated	nukesActivated	nukeCancels	medicHits	ownMedicHits	medicNukes	scoutRapid	lifeBoost	ammoBoost	livesLeft	shotsLeft	penalties	shot3Hit	ownNukeCancels	shotOpponent	shotTeam	missiledOpponent	missiledTeam
          let index = output.entities.findIndex(obj => obj.ipl_id == record[1]);
          output.entities[index].shotsHit = record[2];
          output.entities[index].shotsFired = record[3];
          output.entities[index].timesZapped = record[4];
          output.entities[index].timesMissiled = record[5];
          output.entities[index].missileHits = record[6];
          output.entities[index].nukesDetonated = record[7];
          output.entities[index].nukesActivated = record[8];
          output.entities[index].nukeCancels = record[9];
          output.entities[index].medicHits = record[10];
          output.entities[index].ownMedicHits = record[11];
          output.entities[index].medicNukes = record[12];
          output.entities[index].scoutRapid = record[13];
          output.entities[index].lifeBoost = record[14];
          output.entities[index].ammoBoost = record[15];
          output.entities[index].livesLeft = record[16];
          output.entities[index].shotsLeft = record[17];
          output.entities[index].penalties = record[18];
          output.entities[index].shot3Hit = record[19];
          output.entities[index].ownNukeCancels = record[20];
          output.entities[index].shotOpponent = record[21];
          output.entities[index].shotTeam = record[22];
          output.entities[index].missiledOpponent = record[23];
          output.entities[index].missiledTeam = record[24];
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

    const resultParams = {
      Bucket: bucket,
      Key: key.replace(".tdf", ".json"),
      Body: JSON.stringify(output, null, 4)
    };
    await s3
      .putObject(resultParams, function(err, data) {
        if (err) console.log(err, err.stack);
        // an error occurred
        else console.log(data); // successful response
      })
      .promise();
  } catch (err) {
    console.log("an error has occurred");
  }

  console.log("CHOMP COMPLETE");
};
