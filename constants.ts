import { EntityType, EntityDefault, EntityState } from "types";

export const UIColors: { [index: number]: string } = {
  [0]: "gray", //None
  [1]: "red", //Red
  [2]: "green", //Green
  [3]: "yellow", //Yellow
  [4]: "blue", //Blue
  [5]: "teal", //Aqua
  [6]: "purple", //Purple
  [7]: "gray", //White
  [8]: "orange", //Orange
  [9]: "pink", //Pink
  [10]: "black", //Black
  [11]: "orange", //Fire
  [12]: "cyan", //Ice
  [13]: "green", //Earth
  [14]: "cyan", //Crystal
  [15]: "orange", //Rainbow
};

export const entityTypes: { [index: number]: EntityType } = {
  [1]: EntityType.Commander,
  [2]: EntityType.Heavy,
  [3]: EntityType.Scout,
  [4]: EntityType.Ammo,
  [5]: EntityType.Medic,
};

export const positionDefaults: { [index: string]: EntityDefault } = {
  [EntityType.Commander]: {
    initialShots: 30,
    maxShots: 60,
    resupplyShots: 5,
    initialLives: 15,
    maxLives: 30,
    resupplyLives: 4,
    initialMissiles: 5,
    shotPower: 2,
    maxHP: 3,
  },
  [EntityType.Heavy]: {
    initialShots: 20,
    maxShots: 40,
    resupplyShots: 5,
    initialLives: 10,
    maxLives: 20,
    resupplyLives: 3,
    initialMissiles: 5,
    shotPower: 3,
    maxHP: 3,
  },
  [EntityType.Scout]: {
    initialShots: 30,
    maxShots: 60,
    resupplyShots: 10,
    initialLives: 15,
    maxLives: 30,
    resupplyLives: 5,
    initialMissiles: 0,
    shotPower: 1,
    maxHP: 1,
  },
  [EntityType.Ammo]: {
    initialShots: 15,
    maxShots: 15,
    resupplyShots: 0,
    initialLives: 10,
    maxLives: 20,
    resupplyLives: 3,
    initialMissiles: 0,
    shotPower: 1,
    maxHP: 1,
  },
  [EntityType.Medic]: {
    initialShots: 15,
    maxShots: 30,
    resupplyShots: 5,
    initialLives: 20,
    maxLives: 20,
    resupplyLives: 0,
    initialMissiles: 0,
    shotPower: 1,
    maxHP: 1,
  },
};

export const defaultInitialState: EntityState = {
  stateTime: 0,
  ipl_id: "",
  lives: 0,
  shots: 0,
  currentHP: 0,
  isFinal: false,
  score: 0,
  isActive: true,
  isNuking: false,
  isEliminated: false,
  lastDeacTime: null,
  lastDeacType: null,
  isRapid: false,
  shotsFired: 0,
  shotsHit: 0,
  shotTeam: 0,
  deacTeam: 0,
  shot3Hit: 0,
  deac3Hit: 0,
  shotOpponent: 0,
  deacOpponent: 0,
  shotBase: 0,
  missBase: 0,
  destroyBase: 0,
  medicHits: 0,
  ownMedicHits: 0,
  selfHit: 0,
  selfDeac: 0,
  missileBase: 0,
  missileTeam: 0,
  missileOpponent: 0,
  missilesLeft: 0,
  selfMissile: 0,
  spSpent: 0,
  spEarned: 0,
  resupplyShots: 0,
  selfResupplyShots: 0,
  resupplyLives: 0,
  selfResupplyLives: 0,
  ammoBoosts: 0,
  lifeBoosts: 0,
  ammoBoostedPlayers: 0,
  lifeBoostedPlayers: 0,
  rapidFires: 0,
  shotsFiredDuringRapid: 0,
  shotsHitDuringRapid: 0,
  shotTeamDuringRapid: 0,
  deacTeamDuringRapid: 0,
  shot3HitDuringRapid: 0,
  deac3HitDuringRapid: 0,
  shotOpponentDuringRapid: 0,
  deacOpponentDuringRapid: 0,
  medicHitsDuringRapid: 0,
  selfHitDuringRapid: 0,
  selfDeacDuringRapid: 0,
  selfMissileDuringRapid: 0,
  nukesActivated: 0,
  nukesDetonated: 0,
  nukeMedicHits: 0,
  ownNukeCanceledByNuke: 0,
  ownNukeCanceledByGameEnd: 0,
  ownNukeCanceledByTeam: 0,
  ownNukeCanceledByResupply: 0,
  ownNukeCanceledByOpponent: 0,
  ownNukeCanceledByPenalty: 0,
  ammoBoostReceived: 0,
  lifeBoostReceived: 0,
  cancelOpponentNuke: 0,
  cancelTeamNuke: 0,
  cancelTeamNukeByResupply: 0,
  uptime: 0,
  resupplyDowntime: 0,
  nukeDowntime: 0,
  teamDeacDowntime: 0,
  oppDeacDowntime: 0,
  penaltyDowntime: 0,
  penalties: 0,
};
