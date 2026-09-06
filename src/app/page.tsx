import { Suspense } from "react";

import { ScanWorkbench } from "@/components/scan/scan-workbench";

export default function Home() {
  return (
    <Suspense>
      <ScanWorkbench />
    </Suspense>
  );
}
