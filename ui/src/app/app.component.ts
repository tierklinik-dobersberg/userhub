import { animate, style, transition, trigger } from '@angular/animations';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import {
  ApplicationRef,
  ChangeDetectorRef,
  Component,
  HostListener,
  OnDestroy,
  OnInit,
  inject,
  isDevMode
} from '@angular/core';
import { ActivatedRoute, NavigationEnd, Router } from '@angular/router';
import { SwUpdate } from '@angular/service-worker';
import { Profile } from '@tkd/apis';
import { NzMessageService } from 'ng-zorro-antd/message';
import { NzModalService } from 'ng-zorro-antd/modal';
import { BehaviorSubject, Subject, combineLatest, interval, of } from 'rxjs';
import {
  catchError,
  delay,
  filter,
  first,
  map,
  mergeMap,
  retryWhen,
  share,
  startWith,
  switchMap,
  takeUntil
} from 'rxjs/operators';
import { LayoutService } from 'src/app/services';
import {
  ConfigAPI, Overwrite,
  RosterAPI,
  UIConfig,
  UserService,
  VoiceMailAPI
} from './api';
import { InfoScreenAPI } from './api/infoscreen.api';
import { TkdCreateOfftimeRequestComponent } from './pages/offtime/create-offtime-request';
import { ProfileService } from './services/profile.service';
import { toggleRouteQueryParamFunc } from './utils';

interface MenuEntry {
  Icon: string;
  Link: string;
  Text: string;
  BlankTarget: boolean;
}

interface SubMenu {
  Text: string;
  Items: MenuEntry[];
}

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
  animations: [
    trigger('moveInOut', [
      transition('void => *', [
        style({
          transform: 'translateX(-100%)',
          opacity: 0,
          position: 'absolute',
        }),
        animate('150ms ease-in-out', style({
          transform: 'translateX(0%)',
          opacity: 1
        }))
      ]),
      transition('* => void', [
        animate('150ms ease-in-out', style({
          transform: 'translateX(-100%)',
          opacity: 0,
        }))
      ]),
    ])
  ]
})
export class AppComponent implements OnInit, OnDestroy {
  /** emits when the component is destroyed */
  private destroy$ = new Subject<void>();

  isCollapsed = false;
  isDevMode = isDevMode();

  profileService = inject(ProfileService);
  cdr = inject(ChangeDetectorRef);
  profile?: Profile;


  rootLinks: MenuEntry[] = [];
  subMenus: SubMenu[] = [];
  menuMode: 'inline' | 'vertical' = 'inline';
  mailboxes: string[] = [];

  isReachable: boolean = true;
  checkRunning = false;

  /** The target of the current roster overwrite if any */
  overwriteTarget = '';

  /** Used to trigger a reload of the current overwrite target */
  private reloadOverwrite$ = new BehaviorSubject<void>(undefined);

  isRoster2 = this.router.events.pipe(
    filter((e) => e instanceof NavigationEnd),
    map(() => this.router.url.startsWith('/roster/plan')),
    share()
  );

  /** Returns true if the user has (at least read-only) access to the roster */
  get hasRoster(): boolean {
    return true
  }

  /** Returns true if the user can see voicemail records */
  get hasVoiceMail(): boolean {
    return true
  }

  /** Returns true if the user can see calllogs */
  get hasCallLog(): boolean {
    return true
  }

  /** Returns true if the user can see customer records */
  get hasCustomers(): boolean {
    return true
  }

  get hasInfoScreen(): boolean {
    return false;
  }

  /**
   * Returns true if the user can create calendar events. For usability,
   * this also requires hasCustomer, hasCalllog and hasRoster
   */
  get canCreateEvent(): boolean {
    return true;
  }

  infoScreenEnabled = false;

  constructor(
    private configapi: ConfigAPI,
    private router: Router,
    private activeRoute: ActivatedRoute,
    private nzMessage: NzMessageService,
    private modal: NzModalService,
    private appRef: ApplicationRef,
    private updates: SwUpdate,
    private roster: RosterAPI,
    public layout: LayoutService,
    private voice: VoiceMailAPI,
    private http: HttpClient,
    private userService: UserService,
    private showAPI: InfoScreenAPI,
  ) {}

  readonly toggleMenu = toggleRouteQueryParamFunc(
    this.router,
    this.activeRoute,
    'show-menu'
  );

  ngOnDestroy() {
    this.destroy$.next();
    this.destroy$.complete();
  }

  ngOnInit(): void {
    this.destroy$ = new Subject();

    // watch the user profile.
    this.profileService
      .profile$
      .pipe(takeUntil(this.destroy$))
      .subscribe(profile => {
        this.profile = profile;
        this.cdr.markForCheck();
      });

    combineLatest([interval(15000), this.reloadOverwrite$])
      .pipe(
        startWith(-1),
        takeUntil(this.destroy$),
        switchMap(() =>
          this.roster.getActiveOverwrite().pipe(
            catchError((err) => {
              if (!(err instanceof HttpErrorResponse) || err.status !== 404) {
                console.error(err);
              }
              return of(null as Overwrite);
            })
          )
        )
      )
      .subscribe((overwrite) => {
        this.overwriteTarget = '';
        if (!!overwrite) {
          if (!!overwrite.userId) {
            this.overwriteTarget = this.userService.byId(
              overwrite.userId
            ).user.username
          } else {
            this.overwriteTarget =
              overwrite.displayName || overwrite.phoneNumber;
          }
        }
      });

    this.checkReachability();
    this.activeRoute.queryParamMap
      .pipe(takeUntil(this.destroy$))
      .subscribe((params) => {
        this.isCollapsed = !params.has('show-menu');
      });

    this.updates.activated.subscribe((event) => {
      this.nzMessage.info(
        'Gratuliere! Du verwendest nun die neuste Version von CIS'
      );
    });

    if (!isDevMode()) {
      this.appRef.isStable
        .pipe(
          first((stable) => !!stable),
          mergeMap(() => interval(10 * 60 * 1000).pipe(startWith(-1)))
        )
        .subscribe(() => {
          this.updates.checkForUpdate();
        });
    }

    this.layout.change.pipe(takeUntil(this.destroy$)).subscribe(() => {
      this.isCollapsed = !this.layout.isTabletLandscapeUp;
    });

    this.configapi.change
      .pipe(takeUntil(this.destroy$))
      .subscribe((cfg) => this.applyConfig(cfg));

    this.isCollapsed = this.layout.isPhone;
  }

  @HostListener('window:focus')
  checkReachability() {
    if (this.checkRunning) {
      return;
    }
    this.checkRunning = true;
    this.http
      .get('/api/')
      .pipe(
        retryWhen((d) => {
          this.isReachable = false;
          this.modal.closeAll();
          return d.pipe(delay(2000));
        })
      )
      .subscribe({
        next: () => {
          this.isReachable = true;
          this.checkRunning = false;
        },
        complete: () => {
          console.log('reachability check done');
        },
        error: (err) => console.error(err),
      });
  }

  createOffTimeRequest() {
      this.modal.create({
        nzContent: TkdCreateOfftimeRequestComponent,
        nzFooter: null,
        nzWidth: 'fit-content',
      })
  }

  private applyConfig(cfg: UIConfig | null): void {
    const menus = new Map<string, SubMenu>();
    this.rootLinks = [];

    (cfg?.ExternalLink || []).forEach((link) => {
      if (!link.ParentMenu) {
        this.rootLinks.push(link);
        return;
      }

      let m = menus.get(link.ParentMenu);
      if (!m) {
        m = {
          Text: link.ParentMenu,
          Items: [],
        };
        menus.set(link.ParentMenu, m);
      }

      m.Items.push(link);
    });

    this.subMenus = Array.from(menus.values());
    this.voice.listMailboxes().subscribe((mailboxes) => {
      console.log(mailboxes);
      this.mailboxes = mailboxes;
    });
  }
}
