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

import i18next from 'i18next'
import { initReactI18next } from 'react-i18next'
import LanguageDetector from 'i18next-browser-languagedetector'
import locale from './locale';
import { menuZhAndEnText } from './locale/menu'
const i18n = i18next.createInstance()

const resources = {} as any;

Object.keys(locale).forEach((lang) => {
  resources[lang] = locale[lang].resources;
});

i18n.use(initReactI18next)
  .use(LanguageDetector)
  .init({
    lng: localStorage.getItem('i18nextLng') || (navigator.language === ('en'|| 'en-US') ? 'en-US' : 'zh-CN'),
    fallbackLng: navigator.language === ('en'|| 'en-US') ? 'en-US' : 'zh-CN',
    interpolation: {
      escapeValue: false
    },
    react: {
      useSuspense: false
    },
    defaultNS: 'client',
    resources
  })

export default i18n


// 遍历进行文本替换
export const replaceTextNodes = (nodeList: any) => {
  const localLanguage = localStorage.getItem('i18nextLng') || (navigator.language === ('en'|| 'en-US') ? 'en-US' : 'zh-CN')

  nodeList.forEach((node: any) => {
    const childNode = node.childNodes[0]
    if (!childNode) {
      return
    }
    if (childNode.nodeType === Node.TEXT_NODE) {
      const translations = menuZhAndEnText[localLanguage];
      const text = childNode.textContent
      childNode.textContent = translations[text] || text;
    } else {
      replaceTextNodes(node.childNodes);
    }
  });
}
