import { EntityMVP, EntityState, MVPModel } from "types";

export default function generateMVP(state: EntityState, mvpModel: MVPModel) {
  let result: EntityMVP = { position: mvpModel.position };
  let mvp = 0;

  for (const prop in mvpModel) {
    if (
      mvpModel[prop] &&
      typeof mvpModel[prop] === "number" &&
      <number>mvpModel[prop] > 0 &&
      state[prop] &&
      typeof state[prop] === "number" &&
      <number>state[prop] > 0
    ) {
      //score needs special handling later
      if (prop !== "score") {
        result[prop] = <number>mvpModel[prop] * <number>state[prop];
      } else {
        result[prop] = <number>state[prop];
      }
    }
  }

  //this isnt working
  console.log(`score: ${result.score}`);
  if (result.score && result.score > mvpModel.scoreThreshold) {
    result.score =
      (<number>result.score - mvpModel.scoreThreshold) * <number>mvpModel.score;
  } else {
    delete result.score;
  }

  if (mvpModel.accuracy > 0) {
    result.accuracy =
      mvpModel.accuracy * (state.shotsHit / Math.max(state.shotsFired, 1));
  }

  if (mvpModel.accuracyDuringRapid > 0) {
    result.accuracyDuringRapid =
      mvpModel.accuracyDuringRapid *
      (state.shotsHitDuringRapid / Math.max(state.shotsFiredDuringRapid, 1));
  }

  if (mvpModel.hitDiff > 0) {
    result.hitDiff =
      mvpModel.hitDiff * (state.shotOpponent / Math.max(state.selfHit, 1));
  }

  if (mvpModel.hitDiffDuringRapid > 0) {
    result.hitDiffDuringRapid =
      mvpModel.hitDiffDuringRapid *
      (state.shotOpponentDuringRapid / Math.max(state.selfHitDuringRapid, 1));
  }

  for (const prop in result) {
    if (typeof result[prop] === "number") {
      mvp += <number>result[prop];
    }
  }

  return { mvpDetails: result, mvpValue: mvp };
}
