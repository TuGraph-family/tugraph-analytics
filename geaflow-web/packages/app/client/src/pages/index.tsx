import { Application } from '@tugraph/openpiece-client';
import i18n, { replaceTextNodes } from '../i18n'

export const app = new Application({
  i18n,
  apiClient: {
    baseURL: process.env.API_BASE_URL,
  },
  dynamicImport: (name: string) => {
    return import(`../plugins/${name}`);
  },
});

export default app.render();

// 临时方案：先临时提供文案，后续 Openpiece 内部会处理菜单的国际化
const timer = setInterval(() => {
  const elements = document.querySelectorAll('li span div span');
  if (elements.length > 0) {
    replaceTextNodes(elements);
    clearInterval(timer)

    elements.forEach(element => {
      element.addEventListener('click', () => {
        setTimeout(() => {
          const newelements = document.querySelectorAll('li span div span');
          replaceTextNodes(newelements);
        }, 200);
      })
    })
  }
}, 100)
