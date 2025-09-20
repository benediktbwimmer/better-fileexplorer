module.exports = {
  content: ['./public/**/*.{html,js}'],
  darkMode: 'media',
  theme: {
    extend: {
      colors: {
        surface: {
          light: '#f4f5f7',
          DEFAULT: '#f4f5f7',
          dark: '#111112',
        },
        panel: {
          light: '#ffffff',
          dark: '#1b1b1d',
        },
        accent: {
          DEFAULT: '#6366f1',
          soft: 'rgba(99, 102, 241, 0.15)',
        },
      },
      boxShadow: {
        panel: '0 20px 60px -25px rgba(15, 23, 42, 0.45)',
      },
    },
  },
  plugins: [],
};
