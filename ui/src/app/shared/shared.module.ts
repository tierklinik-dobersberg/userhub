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
import { NzTableModule } from 'ng-zorro-antd/table';
import { NzToolTipModule } from 'ng-zorro-antd/tooltip';
import { MarkdownModule } from 'ngx-markdown';
import { CallLogTableComponent } from './callog-table';
import { CommentComponent } from './comment';
import { HeaderTitleOutletComponent } from './header-title';
import { DurationPipe } from './pipes';
import { CanDeleteCustomerPipe } from './pipes/can-delete-customer';
import { TextInputComponent } from './text-input';

@NgModule({
  imports: [
    NzTableModule,
    NzToolTipModule,
    NzIconModule,
    NzAvatarModule,
    NzMessageServiceModule,
    NzButtonModule,
    NzInputModule,
    NzSelectModule,
    MarkdownModule.forChild(),
    CommonModule,
    FormsModule,
    RouterModule,
    CKEditorModule,
  ],
  declarations: [
    DurationPipe,
    HeaderTitleOutletComponent,
    CallLogTableComponent,
    CommentComponent,
    TextInputComponent,
    CanDeleteCustomerPipe,
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
  ],
})
export class SharedModule { }
