export function getUrlParam(param: string, location?: any) {
  const urlSearchParams = new URLSearchParams(!location ? location.search : window.location.search);
  return urlSearchParams.get(param);
}

export const getLocalData = (key: string) => {
  if (!key) {
    return;
  }
  try {
    const data = JSON.parse(localStorage.getItem(key) || '{}');
    return data;
  } catch (e) {
    console.error(`geaflow ${key} %d ${e}`);
  }
};

export const setLocalData = (key: string, data: any) => {
  if (!key) {
    return;
  }
  localStorage.setItem(key, data);
};

export const convertMillisecondsToHMS = (milliseconds: number) => {
  if (milliseconds < 1000) {
    return `${milliseconds}毫秒`
  }

  // 计算总共有多少秒
  const totalSeconds = Math.floor(milliseconds / 1000);

  // 计算有多少小时
  let hours = Math.floor(totalSeconds / 3600);

  // 剩余的秒数
  let secondsLeft = totalSeconds % 3600;

  // 计算分钟数
  let minutes = Math.floor(secondsLeft / 60);

  // 剩余的秒数
  let seconds = secondsLeft % 60;

  if (hours < 1) {
    // 没有小时，显示分钟和秒
    if (minutes < 1) {
      // 没有分钟，显示秒
      return `${seconds}秒`
    }
    return `${minutes}分${seconds}秒`
  }
  
  return `${hours}小时${minutes}分${seconds}秒`

}