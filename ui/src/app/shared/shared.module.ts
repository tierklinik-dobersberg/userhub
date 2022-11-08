import { ScrollingModule } from '@angular/cdk/scrolling';
import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterModule } from '@angular/router';
import { CKEditorModule } from '@ckeditor/ckeditor5-angular';
import { NzAvatarModule } from 'ng-zorro-antd/avatar';
import { NzButtonModule } from 'ng-zorro-antd/button';
import { NzCheckboxModule } from 'ng-zorro-antd/checkbox';
import { NzEmptyModule } from 'ng-zorro-antd/empty';
import { NzIconModule } from 'ng-zorro-antd/icon';
import { NzInputModule } from 'ng-zorro-antd/input';
import { NzMessageServiceModule } from 'ng-zorro-antd/message';
import { NzModalModule } from 'ng-zorro-antd/modal';
import { NzSelectModule } from 'ng-zorro-antd/select';
import { NzDatePickerModule, NzRangePickerComponent } from 'ng-zorro-antd/date-picker';
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { MarkdownModule } from 'ngx-markdown';
import { TimeagoModule } from 'ngx-timeago';
import { CallLogTableComponent } from './callog-table';
import { CommentComponent } from './comment';
import { HeaderTitleOutletComponent } from './header-title';
import { DurationPipe } from './pipes';
import { CanDeleteCustomerPipe } from './pipes/can-delete-customer';
import { TextInputComponent } from './text-input';
import { NzTimePickerModule } from 'ng-zorro-antd/time-picker';
import { LinkNoBubbleDirective } from './a-no-bubble.directive';
import { MatBottomSheetModule } from '@angular/material/bottom-sheet';
import { TkdOptionSpecInputComponent } from './option-spec-input';
import { TkdStringSliceInputComponent } from './simple-string-slice-input';
import { TkdOptionListInputComponent } from './option-list-input';
import { ListActionButtonGroupComponent } from './list-btn-group';
import { UserAvatarComponent } from './user-avatar';
import { TkdCreateOfftimeRequestComponent } from '../pages/offtime/create-offtime-request';
import { TkdDebounceEventDirective } from './debounce-event.directive';

@NgModule({
  imports: [
    NzTableModule,
    NzToolTipModule,
    NzIconModule,
    NzAvatarModule,
    NzMessageServiceModule,
    NzButtonModule,
    NzInputModule,
    NzCheckboxModule,
    NzSelectModule,
    MarkdownModule.forChild(),
    CommonModule,
    FormsModule,
    RouterModule,
    CKEditorModule,
    TimeagoModule,
  ],
  declarations: [
    DurationPipe,
    HeaderTitleOutletComponent,
    CallLogTableComponent,
    CommentComponent,
    TextInputComponent,
    CanDeleteCustomerPipe,
    LinkNoBubbleDirective,
    TkdStringSliceInputComponent,
    TkdOptionSpecInputComponent,
    TkdOptionListInputComponent,
    ListActionButtonGroupComponent,
    TkdDebounceEventDirective,
    UserAvatarComponent,
  ],
  exports: [
    CommonModule,
    FormsModule,
    RouterModule,
    NzTableModule,
    NzToolTipModule,
    NzIconModule,
    NzAvatarModule,
    NzMessageServiceModule,
    NzButtonModule,
    NzInputModule,
    NzCheckboxModule,
    NzDatePickerModule,
    NzTimePickerModule,
    NzMessageServiceModule,
    NzEmptyModule,
    NzSelectModule,
    ScrollingModule,
    NzModalModule,
    DurationPipe,
    HeaderTitleOutletComponent,
    CommentComponent,
    CallLogTableComponent,
    TextInputComponent,
    CanDeleteCustomerPipe,
    LinkNoBubbleDirective,
    MatBottomSheetModule,
    TkdStringSliceInputComponent,
    TkdOptionSpecInputComponent,
    TkdOptionListInputComponent,
    ListActionButtonGroupComponent,
    UserAvatarComponent,
    TimeagoModule,
    TkdDebounceEventDirective,
  ],
})
export class SharedModule {}
