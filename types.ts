import { float } from "aws-sdk/clients/lightsail";

interface lfstats {
  lfstatsId: number | null;
}

export enum DeacType {
  Resupply = "resupply",
  Nuke = "nuke",
  Opponent = "opponent",
  Team = "team",
  Penalty = "penalty",
}

export enum EntityType {
  Commander = "Commander",
  Heavy = "Heavy Weapons",
  Scout = "Scout",
  Ammo = "Ammo Carrier",
  Medic = "Medic",
}

export interface Center {
  id: number;
  name: string;
  regionCode: number;
  siteCode: number;
}

export interface Game extends lfstats {
  missionType: string;
  missionDesc: string;
  missionStart: number;
  missionStartTime: string;
  missionDuration: number;
  missionDurationMillis: number;
  missionLength: number | null;
  missionLengthMillis: number | null;
  penaltyValue: number;
}

export interface GameMetaData {
  fileVersion: string;
  programVersion: string;
  regionCode: string;
  siteCode: string;
  chomperVersion: string;
  tdfKey: string;
}

export interface Team extends lfstats {
  index: number;
  desc: string;
  colorEnum: number;
  colorDesc: string;
  uiColor: string;
  isEliminated: boolean;
  oppEliminated: boolean;
  elimBonus: number;
}

export interface Entity extends lfstats, EntityDefault {
  startTime: number;
  endTime: number | null;
  iplId: string;
  type: string;
  desc: string;
  team: number;
  level: number;
  category: number;
  position: EntityType;
  battlesuit: string | null;
  endCode: string | null;
  gameTeamId: number | null; //database id for team object
  initialState: EntityState;
  finalState: EntityState | null;
}

export interface EntityDefault {
  initialShots: number;
  maxShots: number;
  resupplyShots: number;
  initialLives: number;
  maxLives: number;
  resupplyLives: number;
  initialMissiles: number;
  shotPower: number;
  maxHP: number;
}

export interface EntityState {
  stateTime: number;
  isFinal: boolean;
  iplId: string;
  score: number;
  isActive: boolean;
  isNuking: boolean;
  isEliminated: boolean;
  lives: number;
  shots: number;
  currentHP: number;
  lastDeacTime: number | null;
  lastDeacType: DeacType | null;
  isRapid: boolean;
  shotsFired: number;
  shotsHit: number;
  shotTeam: number;
  deacTeam: number;
  shot3Hit: number;
  deac3Hit: number;
  shotOpponent: number;
  deacOpponent: number;
  assists: number;
  shotBase: number;
  missBase: number;
  destroyBase: number;
  awardBase: number;
  medicHits: number;
  ownMedicHits: number;
  selfHit: number;
  selfDeac: number;
  missileBase: number;
  missileTeam: number;
  missileOpponent: number;
  missilesLeft: number;
  selfMissile: number;
  spSpent: number;
  spEarned: number;
  resupplyShots: number;
  selfResupplyShots: number;
  selfResupplyLives: number;
  resupplyLives: number;
  ammoBoosts: number;
  lifeBoosts: number;
  ammoBoostedPlayers: number;
  lifeBoostedPlayers: number;
  rapidFires: number;
  shotsFiredDuringRapid: number;
  shotsHitDuringRapid: number;
  shotTeamDuringRapid: number;
  deacTeamDuringRapid: number;
  shot3HitDuringRapid: number;
  deac3HitDuringRapid: number;
  shotOpponentDuringRapid: number;
  deacOpponentDuringRapid: number;
  assistsDuringRapid: number;
  medicHitsDuringRapid: number;
  selfHitDuringRapid: number;
  selfDeacDuringRapid: number;
  selfMissileDuringRapid: number;
  nukesActivated: number;
  nukesDetonated: number;
  nukeMedicHits: number;
  ownNukeCanceledByNuke: number;
  ownNukeCanceledByGameEnd: number;
  ownNukeCanceledByTeam: number;
  ownNukeCanceledByResupply: number;
  ownNukeCanceledByOpponent: number;
  ownNukeCanceledByPenalty: number;
  ammoBoostReceived: number;
  lifeBoostReceived: number;
  cancelOpponentNuke: number;
  cancelTeamNuke: number;
  cancelTeamNukeByResupply: number;
  uptime: number;
  resupplyDowntime: number;
  nukeDowntime: number;
  teamDeacDowntime: number;
  oppDeacDowntime: number;
  penaltyDowntime: number;
  penalties: number;
}

export interface EntityMVP
  extends Omit<
    EntityState,
    | "stateTime"
    | "isFinal"
    | "iplId"
    | "isActive"
    | "isNuking"
    | "isEliminated"
    | "currentHP"
    | "lastDeacTime"
    | "lastDeacType"
    | "isRapid"
  > {
  position: string;
  scoreThreshold: number;
  accuracy: number;
  accuracyDuringRapid: number;
  hitDiff: number;
  hitDiffDuringRapid: number;
  isEliminated: number;
}

export interface GameAction {
  time: number;
  type: string;
  player: string | null;
  action: string | null;
  target: string | null;
  state: Map<string, EntityState> | null;
}
