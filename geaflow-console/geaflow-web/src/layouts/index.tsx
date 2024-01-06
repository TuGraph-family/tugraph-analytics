import { Link, Outlet, useLocation } from "umi";
import Header from "./Header";
import React, { useEffect, useState } from "react";
import styles from "./index.less";
import "../i18n";

export default function Layout() {
  const [isStick, setIsStick] = useState<boolean>(false);
  const handleScroll = () => {
    if (Number(window.scrollY) > 0) {
      setIsStick(true);
    } else {
      setIsStick(false);
    }
  };
  useEffect(() => {
    window.addEventListener("scroll", handleScroll, true);
    return () => {
      window.removeEventListener("scroll", handleScroll, true);
    };
  }, []);

  const location = useLocation();
  let Container =
    ["/", "/login", "/register"].indexOf(
      location.pathname
    ) === -1;
  return (
    <div className={styles.navs}>
      {Container && <Header isStick={isStick} />}
      <div className={styles.containerWrapper}>
        <Outlet />
      </div>
    </div>
  );
}
