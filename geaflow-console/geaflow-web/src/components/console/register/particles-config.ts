import type { ISourceOptions } from 'tsparticles-engine';
const particlesOptions: ISourceOptions = {
  style: {
    width: '760px',
    height: '580px',
  },
  fpsLimit: 120,
  fullScreen: {
    enable: false,
  },
  interactivity: {
    events: {
      onClick: {
        enable: true,
        mode: 'push',
      },
      onHover: {
        enable: true,
        mode: 'grab',
      },
      resize: true,
    },
    modes: {
      push: {
        quantity: 4,
      },
      grab: {
        distance: '200',
      },
    },
  },
  particles: {
    color: {
      value: '#7999fa',
    },
    links: {
      color: '#D8DCE9',
      distance: 150,
      enable: true,
      width: 1,
    },
    collisions: {
      enable: false,
    },
    move: {
      direction: 'none',
      enable: true,
      random: false,
      speed: 2,
      straight: false,
    },
    number: {
      density: {
        enable: true,
        area: 800,
      },
      value: 80,
    },
    opacity: {
      value: 0.9,
    },
    shape: {
      type: 'circle',
    },
    size: {
      value: { min: 1, max: 8 },
    },
  },
  detectRetina: true,
};

export default particlesOptions;
