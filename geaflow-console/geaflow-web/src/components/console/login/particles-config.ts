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
