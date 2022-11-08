import { DragDropModule } from "@angular/cdk/drag-drop";
import { OverlayModule } from "@angular/cdk/overlay";
import { NgModule } from "@angular/core";
import { NzTableModule } from "ng-zorro-antd/table";
import { NzTabsModule } from "ng-zorro-antd/tabs";
import { ColorTwitterModule } from "ngx-color/twitter";
import { SharedModule } from "src/app/shared/shared.module";
import { TkdConstraintManagementComponent } from "./constraint-management";
import { TkdConstraintDialogComponent } from "./constraint-management/constraint-dialog/constraint-dialog";
import { TkdRosterSettingsComponent } from "./roster-settings";
import { TkdRosterSettingsRoutingModule } from "./roster-settings.routing.module";
import { TkdWorkshiftDialogComponent, TkdWorkShiftEndPipe, TkdWorkshiftManagementComponent } from "./workshift-management";

@NgModule({
    imports: [
        SharedModule,
        NzTabsModule,
        TkdRosterSettingsRoutingModule,
        NzTableModule,
        ColorTwitterModule,
        OverlayModule,
        DragDropModule
    ],
    declarations: [
        TkdRosterSettingsComponent,
        TkdWorkshiftManagementComponent,
        TkdWorkshiftDialogComponent,
        TkdWorkShiftEndPipe,
        TkdConstraintManagementComponent,
        TkdConstraintDialogComponent,
    ],
    exports: [
        TkdRosterSettingsComponent,
        TkdWorkShiftEndPipe,
    ]
})
export class TkdRosterSettingsModule {}
