export enum ExcecuteHistoryTabEnum {
  plan = '执行计划',
  efficiency = '时效分析',
  history = '执行历史',
}
export type ExcecuteHistoryTabKey = keyof typeof ExcecuteHistoryTabEnum;
export interface ExcecuteHistoryTabDesc {
  text: string;
  key: ExcecuteHistoryTabKey;
}
