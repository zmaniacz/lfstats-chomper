const aws = require("aws-sdk");
const readline = require("readline");
const moment = require("moment");
const iconv = require("iconv-lite");
const AutoDetectDecoderStream = require("autodetect-decoder-stream");
const { createPool, sql } = require("slonik");

const targetBucket = process.env.TARGET_BUCKET;
const connectionString = process.env.DATABASE_URL;

const s3 = new aws.S3({ apiVersion: "2006-03-01" });

const pool = createPool(connectionString);

exports.handler = async event => {
  console.log("BEGIN CHOMP");
  console.log("Received event:", JSON.stringify(event, null, 2));

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
            resupplies: 0,
            bases_destroyed: 0,
            hits: new Map()
          };
          entities.set(entity.ipl_id, entity);
        } else if (record[0] == 4) {
          //;4/event	time	type	varies
          let action = {
            time: record[1],
            type: record[2],
            player: null,
            action: null,
            target: null
          };

          if (record[2] == "0100" || record[2] == "0101") {
            action.action = record[3];
          } else {
            action.player = record[3];
            action.action = record[4];
            action.target = typeof record[5] != "undefined" ? record[5] : null;
          }

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

          //sum up total resupplies
          if (record[2] == "0500" || record[2] == "0502") {
            entities.get(record[3]).resupplies += 1;
          }

          //sum up total bases destroyed
          if (record[2] == "0303" || record[2] == "0204") {
            entities.get(record[3]).bases_destroyed += 1;
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
            ...player
          };
          //adjsut for penalties
          if (player.penalties > 0) {
            player.score += record[18] * 1000;
          }
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
        else console.log("MOVED TDF TO ARCHIVE", data); // successful response
      })
      .promise();

    //IMPORT PROCESS
    //TODO
    //log gens and targets somehow in the actions
    await pool.connect(async connection => {
      try {
        await connection.transaction(async client => {
          //Let's see if the game already exists before we start doing too much
          let gameExist = await client.maybeOne(
            sql`SELECT games.id 
              FROM games 
              INNER JOIN centers ON games.center_id=centers.id 
              WHERE game_datetime=${game.starttime} AND centers.ipl_id=${game.center}`
          );

          if (gameExist == null) {
            let center = await client.one(sql`
              SELECT *
              FROM centers 
              WHERE ipl_id=${game.center}
            `);

            //find or create lfstats player IDs
            for (let [key, player] of entities) {
              if (player.type == "player" && player.ipl_id.startsWith("@")) {
                //not a member, so assign the generic player id
                player.lfstats_id = 0;
              } else if (
                player.type == "player" &&
                player.ipl_id.startsWith("#")
              ) {
                //member!
                let playerRecord = await client.maybeOne(sql`
                  SELECT *
                  FROM players
                  WHERE ipl_id=${player.ipl_id}
                `);

                if (playerRecord != null) {
                  //IPL exists, let's save the lfstats id
                  player.lfstats_id = playerRecord.id;
                  //set all aliases inactive
                  await client.query(sql`
                    UPDATE players_names 
                    SET is_active=false 
                    WHERE player_id=${player.lfstats_id}
                  `);

                  //Is the player using a new alias?
                  let playerNames = await client.maybeOne(sql`
                    SELECT * 
                    FROM players_names 
                    WHERE players_names.player_id=${player.lfstats_id} AND players_names.player_name=${player.desc}
                  `);

                  if (playerNames == null) {
                    //this is a new alias! why do people do this. i've used one name since 1997. commit, people.
                    //insert the new alias and make it active
                    await client.query(sql`
                    INSERT INTO players_names (player_id,player_name,is_active) 
                    VALUES (${player.lfstats_id}, ${player.desc}, true)
                  `);
                  } else {
                    //existing alias, make it active
                    await client.query(sql`
                    UPDATE players_names 
                    SET is_active=true 
                    WHERE player_id=${player.lfstats_id} AND players_names.player_name=${player.desc}
                  `);
                  }
                  //update the player record with the new active alias
                  await client.query(sql`
                    UPDATE players 
                    SET player_name=${player.desc} 
                    WHERE id=${player.lfstats_id}
                  `);
                } else {
                  //IPL doesn't exist, so let's see if this player name already exists and tie the IPL to an existing record
                  //otherwise create a BRAND NEW player
                  let existingPlayer = await client.maybeOne(sql`
                  SELECT * 
                  FROM players_names 
                  WHERE player_name=${player.desc}
                `);

                  if (existingPlayer != null) {
                    //Found a name, let's use it
                    player.lfstats_id = existingPlayer.player_id;
                    await client.query(sql`
                    UPDATE players 
                    SET ipl_id=${player.ipl_id} 
                    WHERE id=${player.lfstats_id}
                  `);
                  } else {
                    //ITS A FNG
                    let newPlayer = await client.query(sql`
                      INSERT INTO players (player_name,ipl_id) 
                      VALUES (${player.desc},${player.ipl_id})
                      RETURNING *
                    `);
                    player.lfstats_id = newPlayer.rows[0].id;
                    await client.query(sql`
                      INSERT INTO players_names (player_id,player_name,is_active) 
                      VALUES (${player.lfstats_id}, ${player.desc}, true)
                    `);
                  }
                }
              }
            }

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
            game.name = `Game @ ${moment(game.starttime).format("HH:mm")}`;

            let gameRecord = await client.query(sql`
            INSERT INTO games 
                (game_name,game_description,game_datetime,game_length,red_score,green_score,red_adj,green_adj,winner,red_eliminated,green_eliminated,type,center_id)
              VALUES
                (${game.name},'',${game.starttime},${game.gameLength},${redTeam.score},${greenTeam.score},${redBonus},${greenBonus},${winner},${redElim},${greenElim},'social',${center.id})
              RETURNING *
            `);
            let newGame = gameRecord.rows[0];

            //insert the actions
            for (let action of actions) {
              await client.query(sql`
                INSERT INTO game_actions
                  (action_time, action_type, action_text, player, target, game_id) 
                VALUES
                  (${action.time}, ${action.type}, ${action.action}, ${action.player}, ${action.target}, ${newGame.id})
              `);
            }

            //insert the score deltas
            for (const delta of score_deltas) {
              await client.query(sql`
                INSERT INTO score_deltas
                  (score_time, old, delta, new, ipl_id, player_id, game_id) 
                VALUES
                  (${delta.time},${delta.old}, ${delta.delta}, ${delta.new}, ${delta.player}, null, ${newGame.id})
              `);
            }

            //insert the scorecards
            for (const [key, player] of entities) {
              if (player.type == "player") {
                player.normal_team = teams.get(player.team).normal_team;

                let team_elim = 0;
                let elim_other_team = 0;
                if (
                  (redElim && player.normal_team == "red") ||
                  (greenElim && player.normal_team == "green")
                )
                  team_elim = 1;
                if (
                  (redElim && player.normal_team == "green") ||
                  (greenElim && player.normal_team == "red")
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
                      penalties,
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
                      center_id
                    )
                  VALUES
                    (
                      ${player.desc},
                      ${game.starttime},
                      ${player.normal_team},
                      ${player.position},
                      ${player.survived},
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
                      'social',
                      ${player.lfstats_id},
                      ${center.id}
                    )
                    RETURNING *
                `);
                player.scorecard_id = scorecardRecord.rows[0].id;
              }
            }

            //Let's iterate through the entities and make some udpates in the database
            for (let [key, player] of entities) {
              if (player.type == "player") {
                //1-Tie an internal lfstats id to players and targets in each action
                await client.query(sql`
                  UPDATE game_actions
                  SET player_id = ${player.lfstats_id}
                  WHERE player = ${player.ipl_id}
                    AND
                        game_id = ${newGame.id}
                `);
                await client.query(sql`
                  UPDATE game_actions
                  SET target_id = ${player.lfstats_id}
                  WHERE target = ${player.ipl_id}
                    AND
                        game_id = ${newGame.id}
                `);
                //2-Tie an internal lfstats id to each score delta
                await client.query(sql`
                  UPDATE score_deltas
                  SET player_id = ${player.lfstats_id}
                  WHERE ipl_id = ${player.ipl_id}
                    AND
                        game_id = ${newGame.id}
                `);
                //3-insert the hit and missile stats for each player
                for (let [key, target] of player.hits) {
                  if (entities.has(target.ipl_id)) {
                    target.target_lfstats_id = entities.get(
                      target.ipl_id
                    ).lfstats_id;
                  }
                  await client.query(sql`
                  INSERT INTO hits
                    (player_id, target_id, hits, missiles, scorecard_id)
                  VALUES
                    (${player.lfstats_id}, ${target.target_lfstats_id}, ${target.hits}, ${target.missiles}, ${player.scorecard_id})
                `);
                }
                //4-fix penalties
                if (player.penalties > 0) {
                  let penalties = await client.many(sql`
                    SELECT *
                    FROM score_deltas
                    WHERE game_id = ${newGame.id}
                      AND
                        player_id = ${player.lfstats_id}
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

                    //Now the tricky bit, have to rebuild the score deltas from the point the penalty occurred
                    //update the delta event to remove the -1000
                    await client.query(sql`
                      UPDATE score_deltas 
                      SET delta=0,new=new+1000 
                      WHERE id=${penalty.id}
                    `);
                    //Now update a lot of rows, so scary
                    await client.query(sql`
                      UPDATE score_deltas 
                      SET old=old+1000,new=new+1000 
                      WHERE game_id = ${newGame.id}
                        AND
                          player_id = ${player.lfstats_id}
                        AND
                          score_time>${penalty.score_time}
                    `);
                  }
                }
              }
            }
            //calc mvp - lets fuckin go bro, the good shit aw yiss
            let scorecards = await client.many(sql`
              SELECT *
              FROM scorecards
              WHERE game_id = ${newGame.id}
            `);

            for (const scorecard of scorecards) {
              //instantiate the fuckin mvp object bro
              let mvp = 0;
              let mvpDetails = {
                positionBonus: {
                  name: "Position Score Bonus",
                  value: 0
                },
                missiledOpponent: {
                  name: "Missiled Opponent",
                  value: 0
                },
                acc: {
                  name: "Accuracy",
                  value: 0
                },
                nukesDetonated: {
                  name: "Nukes Detonated",
                  value: 0
                },
                nukesCanceled: {
                  name: "Nukes Canceled",
                  value: 0
                },
                medicHits: {
                  name: "Medic Hits",
                  value: 0
                },
                ownMedicHits: {
                  name: "Own Medic Hits",
                  value: 0
                },
                rapidFire: {
                  name: "Activate Rapid Fire",
                  value: 0
                },
                shoot3Hit: {
                  name: "Shoot 3-Hit",
                  value: 0
                },
                ammoBoost: {
                  name: "Ammo Boost",
                  value: 0
                },
                lifeBoost: {
                  name: "Life Boost",
                  value: 0
                },
                medicSurviveBonus: {
                  name: "Medic Survival Bonus",
                  value: 0
                },
                medicScoreBonus: {
                  name: "Medic Score Bonus",
                  value: 0
                },
                elimBonus: {
                  name: "Elimination Bonus",
                  value: 0
                },
                timesMissiled: {
                  name: "Times Missiled",
                  value: 0
                },
                missiledTeam: {
                  name: "Missiled Team",
                  value: 0
                },
                ownNukesCanceled: {
                  name: "Your Nukes Canceled",
                  value: 0
                },
                teamNukesCanceled: {
                  name: "Team Nukes Canceled",
                  value: 0
                },
                elimPenalty: {
                  name: "Elimination Penalty",
                  value: 0
                },
                penalties: {
                  name: "Penalties",
                  value: 0
                }
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
                    Math.floor((scorecard.score - 2000) / 10) * 0.01,
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

              //medic bonus score point
              if ("Medic" == scorecard.position && scorecard.score >= 3000) {
                mvpDetails.medicScoreBonus.value += 1;
              }

              //accuracy bonus
              mvpDetails.acc.value += Math.round(scorecard.accuracy * 100) / 10;

              //don't get missiled dummy
              mvpDetails.timesMissiled.value += scorecard.times_missiled * -1;

              //missile other people instead
              switch (scorecard.position) {
                case "Commander":
                  mvpDetails.missiledOpponent.value +=
                    scorecard.missiled_opponent;
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

                let nukes = await client.any(sql`
                  SELECT SUM(nukes_canceled) as all_nukes_canceled
                  FROM scorecards
                  WHERE game_id = ${newGame.id} AND team = ${team}
                `);

                if (nukes.all_nukes_canceled > 0) {
                  mvpDetails.ownNukesCanceled.value +=
                    nukes.all_nukes_canceled * -3;
                }
              }

              //make commanders cry
              mvpDetails.nukesCanceled.value += scorecard.nukes_canceled * 3;

              //medic tears are scrumptious
              mvpDetails.medicHits.value += scorecard.medic_hits;

              //dont be a venom
              mvpDetails.ownMedicHits.value += scorecard.own_medic_hits * -1;

              //push the little button
              mvpDetails.rapidFire.value += scorecard.scout_rapid * 0.5;
              mvpDetails.lifeBoost.value += scorecard.life_boost * 2;
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
                Math.floor((scorecard.shot_3hit / 6) * 100) / 100;

              //One time DK, one fucking time.
              mvpDetails.teamNukesCanceled.value +=
                scorecard.own_nuke_cancels * -3;

              //more venom points
              mvpDetails.missiledTeam.value += scorecard.missiled_team * -3;

              //WINNER
              //at least 1 MVP for an elim, increased by 1/60 for each second of time remaining over 60
              if (scorecard.elim_other_team > 0)
                mvpDetails.elimBonus.value += Math.max(
                  1,
                  (900 - scorecard.game_length) / 60
                );

              //sum it up and insert
              for (const prop in mvpDetails) {
                mvp += mvpDetails[prop].value;
              }

              await client.query(sql`
                UPDATE scorecards
                SET mvp_points=${mvp}, mvp_details=${JSON.stringify(mvpDetails)}
                WHERE id = ${scorecard.id}
              `);
            }
          }
        });
      } catch (e) {
        throw e;
      }
    });

    console.log("CHOMP COMPLETE");
  } catch (err) {
    console.log("CHOMP ERROR", err.stack);
  }
};
