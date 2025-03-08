import { animate, style, transition, trigger } from '@angular/animations';
import { DatePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, effect, inject, signal } from '@angular/core';
import { RouterModule } from '@angular/router';
import { ConnectError } from '@connectrpc/connect';
import {
  lucideCog,
  lucideLayers,
  lucidePlus,
  lucideUser
} from '@ng-icons/lucide';
import { BrnMenuModule } from '@spartan-ng/ui-menu-brain';
import { injectCurrentProfile } from '@tierklinik-dobersberg/angular/behaviors';
import { injectTaskService } from '@tierklinik-dobersberg/angular/connect';
import {
  HlmIconModule,
  provideIcons,
} from '@tierklinik-dobersberg/angular/icon';
import { LayoutService } from '@tierklinik-dobersberg/angular/layout';
import { HlmMenuModule } from '@tierklinik-dobersberg/angular/menu';
import { ToDatePipe } from '@tierklinik-dobersberg/angular/pipes';
import { ListTasksResponse, Task } from '@tierklinik-dobersberg/apis/tasks/v1';
import { toast } from 'ngx-sonner';
import { interval, startWith, Subscription, switchMap } from 'rxjs';
import { injectCurrentConfig } from 'src/app/api';
import { AppMenuComponent } from 'src/app/components/app-menu';
import { AppAvatarComponent } from 'src/app/components/avatar';
import { openProfilePage } from 'src/app/services';
import { AppDoorStatusButtonComponent } from '../door-status-button';
import { HeaderTitleOutletComponent } from '../header-title';
import { AppSheetNavigationComponent } from '../navigation';
import { NavigationService } from '../navigation/navigation.service';
import { AppRedirectEmergencyButtonComponent } from '../redirect-emergency-button/redirect-emergency-button.component';

@Component({
  selector: 'app-header',
  standalone: true,
  templateUrl: './header.component.html',
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [
    HlmMenuModule,
    BrnMenuModule,
    HlmIconModule,
    HeaderTitleOutletComponent,
    AppSheetNavigationComponent,
    AppRedirectEmergencyButtonComponent,
    AppDoorStatusButtonComponent,
    AppAvatarComponent,
    RouterModule,
    AppMenuComponent,
    DatePipe,
    ToDatePipe
  ],
  animations: [
    trigger('scaleInOut', [
      transition('void => *', [
        style({
          transform: 'scale(0%)',
          opacity: 0,
        }),
        animate('150ms ease-in-out', style({
          transform: 'scale(125%)',
          opacity: 1
        })),
        animate('150ms ease-in-out', style({
          transform: 'scale(100%)',
          opacity: 1
        }))
      ]),
      transition('* => void', [
        animate('150ms ease-in-out', style({
          transform: 'scale(125%)',
          opacity: 1
        })),
        animate('150ms ease-in-out', style({
          transform: 'scale(0%)',
          opacity: 0,
        }))
      ]),
    ])
  ],
  providers: [
    ...provideIcons({
      lucideUser,
      lucideLayers,
      lucideCog,
      lucidePlus,
    }),
  ],
})
export class AppHeaderComponent {
  protected readonly layout = inject(LayoutService);
  protected readonly profile = injectCurrentProfile();
  protected readonly showNav = inject(NavigationService).showNav;
  protected readonly config = injectCurrentConfig();
  protected readonly taskService = injectTaskService();
  protected readonly importantTasks = signal<Task[]>([]);

  constructor() {
    let sub = Subscription.EMPTY;

    effect(() => {
      const config = this.config();
      sub.unsubscribe()

      if (config.UI?.ImportantTasksBoard) {
        const sub = interval(10 * 60 * 1000)
          .pipe(
            startWith(-1),
            switchMap(() => {
              return this.taskService
                .filterTasks({
                  boardId: config.UI.ImportantTasksBoard,
                  query: config.UI.ImportantTasksFilter
                })
                .catch(err => {
                  toast.error("Failed to load important tasks", {
                    description: ConnectError.from(err).message
                  })

                  return new ListTasksResponse()
                })
            }),
          )
          .subscribe(res => {
            this.importantTasks.set(res.tasks || [])
          })
      }
    })
  }

  openProfilePage() {
    openProfilePage()
  }
}
