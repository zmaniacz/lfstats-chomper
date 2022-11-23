import { EntityMVP, EntityState, Game, MVPModel, Team } from "types";

export default function generateMVP(
  state: EntityState,
  mvpModel: MVPModel,
  game: Game,
  team: Team
) {
  let result: EntityMVP = { position: mvpModel.position };
  let mvp = 0;

  for (const prop in mvpModel) {
    if (
      mvpModel[prop] &&
      typeof mvpModel[prop] === "number" &&
      <number>mvpModel[prop] != 0 &&
      state[prop] &&
      typeof state[prop] === "number" &&
      <number>state[prop] != 0
    ) {
      //score needs special handling later
      if (prop !== "score") {
        result[prop] = <number>mvpModel[prop] * <number>state[prop];
      } else {
        result[prop] = <number>state[prop];
      }
    }
  }

  if (result.score && result.score > mvpModel.scoreThreshold) {
    result.score =
      (<number>result.score - mvpModel.scoreThreshold) * <number>mvpModel.score;
  } else {
    delete result.score;
  }

  if (mvpModel.accuracy != 0) {
    result.accuracy =
      mvpModel.accuracy * (state.shotsHit / Math.max(state.shotsFired, 1));
  }

  if (mvpModel.accuracyDuringRapid != 0) {
    result.accuracyDuringRapid =
      mvpModel.accuracyDuringRapid *
      (state.shotsHitDuringRapid / Math.max(state.shotsFiredDuringRapid, 1));
  }

  if (mvpModel.hitDiff != 0) {
    result.hitDiff =
      mvpModel.hitDiff * (state.shotOpponent / Math.max(state.selfHit, 1));
  }

  if (mvpModel.hitDiffDuringRapid != 0) {
    result.hitDiffDuringRapid =
      mvpModel.hitDiffDuringRapid *
      (state.shotOpponentDuringRapid / Math.max(state.selfHitDuringRapid, 1));
  }

  if (mvpModel.isEliminated != 0) {
    if (
      (state.position != "Medic" && state.isEliminated) ||
      (state.position === "Medic" && !state.isEliminated)
    ) {
      result.isEliminated = mvpModel.isEliminated * 1;
    }
  }

  if (state.isFinal && team.oppEliminated) {
    //calculate the elim bonus
    if (game.missionLength) {
      result.elimBonus = Math.max(
        mvpModel.elimMinBonus +
          ((game.missionMaxLength -
            game.missionLength -
            mvpModel.elimMinutesRemainingThreshold * 60) *
            mvpModel.elimPerMinuteBonus) /
            60,
        mvpModel.elimMinBonus
      );
    } else {
      //default to 2
      result.elimBonus = mvpModel.elimDefaultBonus;
    }
  }

  for (const prop in result) {
    if (typeof result[prop] === "number") {
      mvp += <number>result[prop];
    }
  }
  //hard coded model id for now
  return { mvpDetails: result, mvpValue: mvp, mvpModelId: 1 };
}
