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
