/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import $i18n from "@/components/i18n";
export function getUrlParam(param: string, location?: any) {
  const urlSearchParams = new URLSearchParams(
    !location ? location.search : window.location.search
  );
  return urlSearchParams.get(param);
}

export const getLocalData = (key: string) => {
  if (!key) {
    return;
  }
  try {
    const data = JSON.parse(localStorage.getItem(key) || "{}");
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
    return $i18n.get(
      {
        id: "openpiece-geaflow.console.geaflow.util.MillisecondsMilliseconds",
        dm: "{milliseconds}毫秒",
      },
      { milliseconds: milliseconds }
    );
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
      return $i18n.get(
        {
          id: "openpiece-geaflow.console.geaflow.util.SecondsSeconds",
          dm: "{seconds}秒",
        },
        { seconds: seconds }
      );
    }
    return $i18n.get(
      {
        id: "openpiece-geaflow.console.geaflow.util.MinutesMinutesSecondsSeconds",
        dm: "{minutes}分{seconds}秒",
      },
      { minutes: minutes, seconds: seconds }
    );
  }

  return $i18n.get(
    {
      id: "openpiece-geaflow.console.geaflow.util.HoursHoursMinutesMinutesSeconds",
      dm: "{hours}小时{minutes}分{seconds}秒",
    },
    { hours: hours, minutes: minutes, seconds: seconds }
  );
};
